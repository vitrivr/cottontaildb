package org.vitrivr.cottontail.dbms.entity

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.*
import org.vitrivr.cottontail.dbms.entity.EntityMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.events.SchemaEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionStatus
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import java.util.*

/**
 * The default [Entity] implementation based on JetBrains Xodus.
 *
 * @see Entity
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 5.0.0
 */
class DefaultEntity(override val name: Name.EntityName, override val parent: DefaultSchema, val entityId: UUID) : Entity {

    /** A [DefaultEntity] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [Environment] backing this [DefaultEntity]. */
    internal val environment = Environments.newInstance(
        this.catalogue.config.dataFolder(this.entityId).toFile(),
        this.catalogue.config.xodus.toEnvironmentConfig()
    )

    /** An internal cache of all ongoing [DefaultSchema.Tx]s for this [DefaultSchema]. */
    private val transactions = Long2ObjectOpenHashMap<DefaultEntity.Tx>()

    /**
     * An internal cache of [Name.IndexName] to [Index].
     *
     * These are cached to avoid re-creating them for every query.
     */
    private val indexes = Object2ObjectLinkedOpenHashMap<Name.IndexName, Index>()

    /**
     * An internal cache of [Name.ColumnName] to [DefaultSequence].
     *
     * These are cached to avoid re-creating them for every query.
     */
    private val columns = Object2ObjectLinkedOpenHashMap<Name.ColumnName, Column<*>>()

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [QueryContext].
     *
     * @param parent The parent [SchemaTx].
     * @return New [DefaultEntity.Tx]
     */
    @Synchronized
    override fun createOrResumeTx(parent: SchemaTx): Tx {
        require(parent is DefaultSchema.Tx) { "A DefaultEntity can only be accessed with a DefaultSchema.Tx!" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     */
    inner class Tx(override val parent: DefaultSchema.Tx) : EntityTx, SubTransaction.WithCommit, SubTransaction.WithFinalization {

        /** Begins a Xodus transaction for this [Tx]. */
        internal val xodusTx = this@DefaultEntity.environment.beginTransaction()

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /** The bitmap store that backs this [DefaultEntity]. */
        private val bitmap = this.xodusTx.environment.openBitmap(this@DefaultEntity.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

        init {
            if (this@DefaultEntity.columns.isEmpty()) {
                /* Load entity metadata. */
                val entityMetadataStore = EntityMetadata.store(this.parent.xodusTx)
                val entityMetadata = entityMetadataStore.get(this.parent.xodusTx, NameBinding.Entity.toEntry(this@DefaultEntity.name))?.let {
                    EntityMetadata.fromEntry(it)
                } ?: throw DatabaseException.EntityDoesNotExistException(name)

                /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
                val columnMetadataStore = ColumnMetadata.store(this.parent.xodusTx)
                for (c in entityMetadata.columns) {
                    val columnName = this@DefaultEntity.name.column(c)
                    val columnEntry = columnMetadataStore.get(this.parent.xodusTx, NameBinding.Column.toEntry(columnName))?.let {
                        ColumnMetadata.fromEntry(it)
                    } ?: throw DatabaseException.DataCorruptionException("Failed to load specified column $columnName for entity ${this@DefaultEntity.name}")
                    val columnDef = ColumnDef(columnName, columnEntry.type, columnEntry.nullable, columnEntry.primary, columnEntry.autoIncrement)
                    this@DefaultEntity.columns[columnName] = if (columnDef.type is Types.Vector<*,*>) {
                        FixedLengthColumn(columnDef, this@DefaultEntity)
                    } else {
                        VariableLengthColumn(columnDef, this@DefaultEntity)
                    }
                }
            }

            /* Load a map of indexes. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            if (this@DefaultEntity.indexes.isEmpty()) {
                val indexMetadataStore = IndexMetadata.store(this.parent.xodusTx)
                indexMetadataStore.openCursor(this.parent.xodusTx).use {
                    if (it.getSearchKeyRange(NameBinding.Entity.toEntry(this@DefaultEntity.name)) != null) {
                        do {
                            val indexName = NameBinding.Index.fromEntry(it.key)
                            if (indexName.entity() != this@DefaultEntity.name) break
                            val indexEntry = IndexMetadata.fromEntry(it.value)
                            this@DefaultEntity.indexes[indexName] = indexEntry.type.descriptor.open(indexName, this.dbo)
                        } while (it.next)
                    }
                }
            }
        }

        /**
         * Map of [Name.IndexName] to [Index]
         *
         * This starts as a local copy of the [DefaultEntity.indexes] but can be modified by this [DefaultSchema.Tx].
         */
        private val indexes = Object2ObjectLinkedOpenHashMap(this@DefaultEntity.indexes)

        /**
         * Map of [Name.IndexName] to [Index]
         *
         * This starts as a local copy of the [DefaultEntity.columns] but can be modified by this [DefaultSchema.Tx].
         */
        private val columns = Object2ObjectLinkedOpenHashMap(this@DefaultEntity.columns)

        /** A [List] of [Event]s that were executed through this [Tx]. */
        private val events = LinkedList<Event>()

        /**
         * Returns true if the [Entity] underpinning this [EntityTx] contains the given [TupleId] and false otherwise.
         *
         * If this method returns true, then [EntityTx.read] will return a [Tuple] for [TupleId]. However, if this method
         * returns false, then [EntityTx.read] will throw an exception for that [TupleId].
         *
         * @param tupleId The [TupleId] of the desired entry
         * @return True if entry exists, false otherwise,
         */
        @Synchronized
        override fun contains(tupleId: TupleId): Boolean {
            return this.bitmap.get(this.xodusTx, tupleId)
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Tuple]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @param columns The [ColumnDef]s that should be read.
         * @return The desired [Tuple].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        @Synchronized
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Tuple {
            require(this.bitmap.get(this.xodusTx, tupleId)) { "Tuple with ID $tupleId does not exist." }

            /* Read values from underlying columns. */
            val values = Array(columns.size) {
                val column = columns[it].name
                val columnTx = this.columns[column]?.newTx(this) ?: throw IllegalArgumentException("Column $column does not exist on entity ${this@DefaultEntity.name}.")
                columnTx.read(tupleId)
            }

            /* Return value of all the desired columns. */
            return StandaloneTuple(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        @Synchronized
        override fun count(): Long = this.bitmap.count(this.xodusTx)

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        @Synchronized
        override fun smallestTupleId(): TupleId = this.bitmap.getFirst(this.xodusTx)

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        @Synchronized
        override fun largestTupleId(): TupleId = this.bitmap.getLast(this.xodusTx)

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        @Synchronized
        override fun listColumns(): List<ColumnDef<*>> = this.columns.values.map { it.columnDef }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        @Synchronized
        override fun columnForName(name: Name.ColumnName): Column<*> {
            val fqn = this@DefaultEntity.name.column(name.simple)
            return this.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
        }

        /**
         * Lists all [Name.IndexName] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        @Synchronized
        override fun listIndexes(): List<Name.IndexName> = this.indexes.keys.toList()

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        @Synchronized
        override fun indexForName(name: Name.IndexName): Index = this.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         * @param configuration The [IndexConfig] to initialize the [Index] with.
         * @return Newly created [Index] for use in context of this [Tx]
         */
        @Synchronized
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: List<Name.ColumnName>, configuration: IndexConfig<*>): Index {
            /* Check if entity already exists. */
            require(name.entity() == this@DefaultEntity.name) { "Index $name does not belong to entity! This is a programmer's error!"}

            /* Prepare index entry and persist it. */
            val store = IndexMetadata.store(this.parent.xodusTx)
            val state = if (this.count() == 0L) {
                type.defaultEmptyState
            } else {
                IndexState.DIRTY
            }
            val indexEntry = IndexMetadata(type, state, columns.map { it.column }, configuration)
            if (!store.add(this.xodusTx, NameBinding.Index.toEntry(name), IndexMetadata.toEntry(indexEntry))) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Initialize index store entry. */
            if (!type.descriptor.initialize(name, this)) {
                throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to initialize store.")
            }

            /* Try to open index. */
            val index = type.descriptor.open(name, this@DefaultEntity)
            this.indexes[name] = index

            /* Signal event to transaction context. */
            val event = IndexEvent.Created(index)
            this.events.add(event)
            this.context.txn.signalEvent(event)

            /* Return index. */
            return index
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        @Synchronized
        override fun dropIndex(name: Name.IndexName) {
            /* Remove index from local map. */
            val index = this.indexes.remove(name) ?: throw DatabaseException.IndexDoesNotExistException(name)

            /* Remove index entry in store. */
            val store = IndexMetadata.store(this.parent.xodusTx)
            if (!store.delete(this.xodusTx, NameBinding.Index.toEntry(name))) {
                this.indexes[name] = index
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed remove metadata entry.")
            }

            /* De-initialize the index. */
            if (!index.type.descriptor.deinitialize(name, this)) {
                this.indexes[name] = index
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to de-initialize store.")
            }

            /* Signal event to transaction context. */
            val event = IndexEvent.Created(index)
            this.events.add(event)
            this.context.txn.signalEvent(event)
        }

        /**
         *
         */
        @Synchronized
        override fun truncate() {
            /* Truncate bitmap. */
            this.xodusTx.environment.truncateStore("${this.dbo.name.storeName()}#bitmap", this.xodusTx)

            /* Reset associated columns & sequences. */
            this.listColumns().forEach {
                /* TODO: Truncate column */

                /* Reset sequence. */
                if (it.autoIncrement) {
                    this.parent.sequenceForName(it.name.autoincrement()!!).newTx(this.parent).reset()
                }
            }

            /* Reset associated indexes. */
            this.listIndexes().forEach {
                val indexTx = this.indexForName(it).newTx(this)
                indexTx.dbo.type.descriptor.deinitialize(it, this)
                indexTx.dbo.type.descriptor.initialize(it,this)
            }
        }

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns
         * all [TupleId]s contained within the surrounding [DefaultEntity].
         *
         * <strong>Important:</strong> It remains to the caller to close the [Iterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param rename An array of [Name.ColumnName] that should be used instead of the actual [Name.ColumnName].
         *
         * @return [Cursor]
         */
        @Synchronized
        override fun cursor(columns: Array<ColumnDef<*>>, rename: Array<Name.ColumnName>): Cursor<Tuple>
            = cursor(columns, this.smallestTupleId()..this.largestTupleId(), rename)

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns all [TupleId]s
         * contained within the surrounding [DefaultEntity] and a certain range.
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param partition The [LongRange] specifying the [TupleId]s that should be scanned.
         * @param rename An array of [Name.ColumnName] that should be used instead of the actual [Name.ColumnName].
         *
         * @return [Cursor]
         */
        @Synchronized
        override fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange, rename: Array<Name.ColumnName>)
            = DefaultEntityCursor(this, columns, partition, rename)

        /**
         * Insert the provided [Tuple].
         *
         * @param tuple The [Tuple] that should be inserted.
         * @return The generated [Tuple].
         *
         * @throws TransactionException If some of the [Tx] on [Column] or [Index] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        @Suppress("UNCHECKED_CAST")
        override fun insert(tuple: Tuple): Tuple {
           /* Execute INSERT on column level. */
            val tupleId = nextTupleId()
            val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this.columns.size)
            this.bitmap.set(this.xodusTx, tupleId, true)
            for (column in this.columns.values) {
                /* Make necessary checks for value. */
                val value = when {
                    column.columnDef.autoIncrement -> {
                        /* Obtain sequence and generate next value. */
                        val sequenceName = column.name.autoincrement() ?: throw IllegalStateException("")
                        val sequenceTx = this.parent.sequenceForName(sequenceName).newTx(this.parent)

                        /* Generate and use value. */
                        when (column.type) {
                            Types.Int -> sequenceTx.next().asInt()
                            Types.Long -> sequenceTx.next()
                            else -> throw IllegalStateException("Columns of types ${column.type} do not allow for serial values. This is a programmer's error!")
                        }
                    }
                    column.columnDef.nullable -> tuple[column.columnDef]
                    else -> tuple[column.columnDef] ?: throw DatabaseException.ValidationException("Cannot INSERT a NULL value into column ${column.columnDef}.")
                }

                /* Record and perform insert. */
                inserts[column.columnDef] = value
                if (value != null) {
                    (column.newTx(this) as ColumnTx<Value>).write(tupleId, value)
                }
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes. */
            val event = DataEvent.Insert(this@DefaultEntity.name, tupleId, inserts)
            for (index in this.indexes.values) {
                index.newTx(this).insert(event)
            }

            /* Signal event to transaction context. */
            this.context.txn.signalEvent(event)

            /* Return generated record. */
            return StandaloneTuple(tupleId, inserts.keys.toTypedArray(), inserts.values.toTypedArray())
        }

        /**
         * Updates the provided [Tuple] (identified based on its [TupleId]). Columns specified in the [Tuple] that are not part
         * of the [DefaultEntity] will cause an error!
         *
         * @param tuple The [Tuple] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        @Suppress("UNCHECKED_CAST")
        @Synchronized
        override fun update(tuple: Tuple) {
            /* Execute UPDATE on column level. */
            val updates = Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?, Value?>>(tuple.columns.size)
            for (def in tuple.columns) {
                val column = this.columns[def.name] ?: throw DatabaseException.ColumnDoesNotExistException(def.name)
                val columnTx = column.newTx(this) as ColumnTx<Value>
                val value = tuple[def]
                val oldValue = if (value == null) {
                    if (!def.nullable) throw DatabaseException.ValidationException("Record ${tuple.tupleId} cannot be updated with NULL value for column $def, because column is not nullable.")
                    columnTx.delete(tuple.tupleId)
                } else {
                    columnTx.write(tuple.tupleId, value)
                }
                updates[def] = Pair(oldValue, value) /* Map: ColumnDef -> Pair[Old, New]. */
            }

            /* Issue DataChangeEvent.UpdateDataChangeEvent and update indexes + statistics. */
            val event = DataEvent.Update(this@DefaultEntity.name, tuple.tupleId, updates)
            for (index in this.indexes.values) {
                index.newTx(this).update(event)
            }

            /* Signal event to transaction context. */
            this.context.txn.signalEvent(event)
        }

        /**
         * Deletes the entry with the provided [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        @Synchronized
        override fun delete(tupleId: TupleId) {
            /* Perform DELETE on column level. */
            val deleted = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.columns.size)
            for (column in this.columns.values) {
                deleted[column.columnDef] = column.newTx(this).delete(tupleId)
            }

            /* Unset tupleId in bitmap. */
            this.bitmap.set(this.xodusTx, tupleId, false)

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val event = DataEvent.Delete(this@DefaultEntity.name, tupleId, deleted)
            for (index in this.indexes.values) {
                index.newTx(this).delete(event)
            }

            /* Signal event to transaction context. */
            this.context.txn.signalEvent(event)
        }

        /**
         * Checks if this [DefaultEntity.Tx] is prepared for commit by comparing the high address of the
         * local Xodus transaction with the high address of the latest snapshot
         */
        @Synchronized
        override fun prepareCommit(): Boolean {
            check(this.context.txn.state == TransactionStatus.PREPARE) { "Transaction ${this.context.txn.transactionId} is in wrong state and cannot be committed." }
            if (this.xodusTx.isIdempotent) return true
            return this.xodusTx.environment.computeInReadonlyTransaction {
                it.highAddress == this.xodusTx.highAddress
            }
        }

        /**
         * Commits the [DefaultEntity.Tx] and persists all changes.
         */
        @Synchronized
        override fun commit() {
            check(this.context.txn.state == TransactionStatus.COMMIT) { "Transaction ${this.context.txn.transactionId} is in wrong state and cannot be committed." }
            if (this.xodusTx.isIdempotent) {
                this.xodusTx.abort()
            } else {
                if (!this.xodusTx.commit()) {
                    throw DatabaseException.DataCorruptionException("Failed to commit transaction in COMMIT phase.")
                }
            }
        }

        /**
         * Obtains the next free [TupleId] based on the entity bitmap.
         *
         * @return Next [TupleId] for insert.
         */
        @Synchronized
        private fun nextTupleId(): TupleId {
            val txn = this.xodusTx
            val smallest = this.bitmap.getFirst(txn)
            val next = when {
                smallest == -1L -> 0L
                smallest > 0L -> smallest - 1L
                else -> this.bitmap.getLast(txn) + 1L
            }
            return next
        }

        /**
         * Materializes local changes in owning [DefaultEntity].
         */
        override fun finalize(commit: Boolean) {
            this@DefaultEntity.transactions.remove(this.context.txn.transactionId)
            if (!commit) return
            for (event in this.events) {
                when (event) {
                    is IndexEvent.Created -> this@DefaultEntity.indexes[event.index.name] = event.index
                    is IndexEvent.Dropped ->  this@DefaultEntity.indexes.remove(event.index.name)
                    else -> { /* No op. */}
                }
            }
        }
    }
}
