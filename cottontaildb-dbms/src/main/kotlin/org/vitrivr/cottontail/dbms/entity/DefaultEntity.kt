package org.vitrivr.cottontail.dbms.entity

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.vfs.VirtualFileSystem
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
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.EntityMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionStatus
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.storage.ool.FixedOOLFile
import org.vitrivr.cottontail.storage.ool.VariableOOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.tuple.StoredTupleSerializer
import java.nio.file.Path
import java.nio.file.Paths
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
    private val environment = Environments.newInstance(
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
    private val columns = Object2ObjectLinkedOpenHashMap<Name.ColumnName, OOLFile<*, *>>()

    /** The location of this [DefaultEntity]'s data files. */
    val location: Path = Paths.get(this@DefaultEntity.environment.location)


    init {
        /* Initialize VirtualFileSystem once. */
        val vfs = VirtualFileSystem(this.environment)
        vfs.shutdown()
    }

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
     * A [Tx] that affects this [DefaultEntity].
     */
    inner class Tx(override val parent: DefaultSchema.Tx) : EntityTx, SubTransaction.WithCommit, SubTransaction.WithFinalization {
        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity = this@DefaultEntity

        /** Begins a Xodus transaction for this [Tx]. */
        internal val xodusTx = this@DefaultEntity.environment.beginTransaction()

        /** The high-address of this [xodusTx] at the start of this [Tx]. */
        private val highAddress = this.xodusTx.highAddress

        /** The store that backs this [DefaultEntity]. */
        private val store = this.xodusTx.environment.openStore(this@DefaultEntity.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

        /**
         * Map of [Name.IndexName] to [Index]
         *
         * This starts as a local copy of the [DefaultEntity.indexes] but can be modified by this [DefaultSchema.Tx].
         */
        private val indexes: Object2ObjectLinkedOpenHashMap<Name.IndexName,Index>

        /** The [ColumnDef] held by this [DefaultEntity] */
        private val columns: Array<ColumnDef<*>>

        /** A [List] of [Event]s that were executed through this [Tx]. */
        private val events = LinkedList<Event>()

        /** The [StoredTupleSerializer] used by this [DefaultEntity]. */
        private val serializer: StoredTupleSerializer

        init {
            /* Load entity metadata. */
            val entityMetadataStore = EntityMetadata.store(this.parent.xodusTx)
            val entityMetadata = entityMetadataStore.get(this.parent.xodusTx, NameBinding.Entity.toEntry(this@DefaultEntity.name))?.let {
                EntityMetadata.fromEntry(it)
            } ?: throw DatabaseException.EntityDoesNotExistException(name)

            /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            val columnMetadataStore = ColumnMetadata.store(this.parent.xodusTx)
            val columns = mutableListOf<ColumnDef<*>>()
            for (c in entityMetadata.columns) {
                /* Read and prepare column metadata. */
                val columnName = this@DefaultEntity.name.column(c)
                val columnEntry = columnMetadataStore.get(this.parent.xodusTx, NameBinding.Column.toEntry(columnName))?.let {
                    ColumnMetadata.fromEntry(it)
                } ?: throw DatabaseException.DataCorruptionException("Failed to load specified column $columnName for entity ${this@DefaultEntity.name}")

                /* Prepare column definition. */
                val column = ColumnDef(columnName, columnEntry.type, columnEntry.nullable, columnEntry.primary, columnEntry.autoIncrement)
                columns.add(column)

                /* Initialize the OOL files where necessary. */
                if (!column.inline) {
                    if (column.type.fixedLength) {
                        this@DefaultEntity.columns.computeIfAbsent(column.name) { name: Name.ColumnName -> FixedOOLFile(this@DefaultEntity.location.resolve(name.column), column.type) }
                    } else {
                        this@DefaultEntity.columns.computeIfAbsent(column.name) { name: Name.ColumnName -> VariableOOLFile(this@DefaultEntity.location.resolve(name.column), column.type) }
                    }
                }
            }
            this.columns = columns.toTypedArray()

            /* Initialize serializer for tuple. */
            this.serializer = StoredTupleSerializer(this.columns, this@DefaultEntity.columns, AccessPattern.RANDOM)

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
            this.indexes = Object2ObjectLinkedOpenHashMap(this@DefaultEntity.indexes)
        }

        /**
         * Gets and returns [ValueStatistics] for the specified [ColumnDef].
         *
         * @return [ValueStatistics].
         */
        override fun statistics(column: ColumnDef<*>): ValueStatistics<*> {
            return this.context.statistics[column.name]?.statistics ?: column.type.defaultStatistics<Value>()
        }

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
            return this.store.get(this.xodusTx, tupleId.toKey()) != null
        }

        /**
         * Reads the values of from this [DefaultEntity] and returns it as a [Tuple]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @return The desired [Tuple].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        @Synchronized
        override fun read(tupleId: TupleId): Tuple {
            /* Read descriptor from store. */
            val descriptorRaw = this.store.get(this.xodusTx, tupleId.toKey()) ?: throw DatabaseException.DataCorruptionException("Failed to read tuple with ID $tupleId.")
            return this.serializer.fromEntry(tupleId, descriptorRaw)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        @Synchronized
        override fun count(): Long = this.store.count(this.xodusTx)

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        @Synchronized
        override fun smallestTupleId(): TupleId = this.store.openCursor(this.xodusTx).use {
            if (it.next) {
                return LongBinding.compressedEntryToLong(it.key)
            } else {
                return -1L
            }
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        @Synchronized
        override fun largestTupleId(): TupleId = this.store.openCursor(this.xodusTx).use {
            if (it.last) {
                return LongBinding.compressedEntryToLong(it.key)
            } else {
                return -1L
            }
        }

        /**
         * Lists all [ColumnDef]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [ColumnDef]s.
         */
        @Synchronized
        override fun listColumns(): List<ColumnDef<*>> = this.columns.toList()

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the column.
         * @return [ColumnDef] of the column.
         */
        @Synchronized
        override fun columnForName(name: Name.ColumnName): ColumnDef<*> {
            val fqn = this@DefaultEntity.name.column(name.simple)
            return this.columns.find { it.name == fqn } ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
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
            if (!store.add(this.parent.xodusTx, NameBinding.Index.toEntry(name), IndexMetadata.toEntry(indexEntry))) {
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
            if (!store.delete(this.parent.xodusTx, NameBinding.Index.toEntry(name))) {
                this.indexes[name] = index
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed remove metadata entry.")
            }

            /* De-initialize the index. */
            if (!index.type.descriptor.deinitialize(name, this)) {
                this.indexes[name] = index
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to de-initialize store.")
            }

            /* Signal event to transaction context. */
            val event = IndexEvent.Dropped(index)
            this.events.add(event)
            this.context.txn.signalEvent(event)
        }

        /**
         * Truncates this [DefaultEntity], thus deleting all entries.
         */
        @Synchronized
        override fun truncate() {
            /* Truncate bitmap. */
            this.xodusTx.environment.truncateStore(this.dbo.name.storeName(), this.xodusTx)

            /* Reset associated columns & sequences. */
            this.listColumns().forEach {
                /* Reset sequence. */
                if (it.autoIncrement) {
                    this.parent.sequenceForName(it.name.autoincrement()!!).newTx(this.parent).reset()
                }
            }

            /* Reset associated indexes. */
            this.listIndexes().forEach {
                val index = this.indexForName(it)
                index.type.descriptor.deinitialize(it, this)
                index.type.descriptor.initialize(it,this)
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
        override fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange, rename: Array<Name.ColumnName>): Cursor<Tuple> {
            val serializer = StoredTupleSerializer(this.columns, this@DefaultEntity.columns, AccessPattern.SEQUENTIAL)
            return DefaultEntityCursor(this, serializer, partition, rename)
        }

        /**
         * Insert the provided [Tuple].
         *
         * @param tuple The [Tuple] that should be inserted.
         * @return The generated [Tuple].
         *
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        @Synchronized
        override fun insert(tuple: Tuple): Tuple {
           /* Execute INSERT on column level. */
            val tupleId = nextTupleId()
            val insertedTuple = StandaloneTuple(0L, this.columns, Array(this.columns.size) {
                val column = this.columns[it]

                /* Make necessary checks for value. */
                val value = when {
                    column.autoIncrement -> {
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
                    column.nullable -> tuple[column]
                    else -> tuple[column] ?: throw DatabaseException.ValidationException("Cannot INSERT a NULL value into column ${column}.")
                }

                /* Return stored value.*/
                value
            })

            /* Store descriptor.  */
            if (!this.store.add(this.xodusTx, tupleId.toKey(), this.serializer.toEntry(insertedTuple))) {
                throw DatabaseException.DataCorruptionException("Failed to INSERT tuple with ID $tupleId because it already exists.")
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes. */
            val event = DataEvent.Insert(this@DefaultEntity.name, insertedTuple)
            for (index in this.indexes.values) {
                index.newTx(this).insert(event)
            }

            /* Signal event to transaction context. */
            this.context.txn.signalEvent(event)

            /* Return generated record. */
            return insertedTuple
        }

        /**
         * Updates the provided [Tuple] (identified based on its [TupleId]). Columns specified in the [Tuple] that are not part
         * of the [DefaultEntity] will cause an error!
         *
         * @param tuple The [Tuple] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        @Synchronized
        override fun update(tuple: Tuple) {
            /* Read existing tuple from store. */
            val oldTupleRaw = this.store.get(this.xodusTx, tuple.tupleId.toKey()) ?: throw DatabaseException.DataCorruptionException("Failed to read tuple with ID $${tuple.tupleId}.")
            val oldTuple = this.serializer.fromEntry(tuple.tupleId, oldTupleRaw)

            /* Prepare update tuple. */
            val updatedTuple = StandaloneTuple(0L, this.columns, Array(this.columns.size) {
                val column = this.columns[it]
                val value = tuple[column.name] ?: oldTuple[column.name]
                if (value == null && !column.nullable) {
                    throw DatabaseException.ValidationException("Record ${oldTuple.tupleId} cannot be updated with NULL value for column $column, because column is not nullable.")
                }
                value
            })

            /* Update tuple in store. */
            this.store.put(this.xodusTx, tuple.tupleId.toKey(), this.serializer.toEntry(updatedTuple))

            /* Issue DataChangeEvent.UpdateDataChangeEvent and update indexes + statistics. */
            val event = DataEvent.Update(this@DefaultEntity.name, oldTuple.materialize(), updatedTuple)
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
            /* Read descriptor from store. */
            val oldTupleRaw = this.store.get(this.xodusTx, tupleId.toKey()) ?: throw DatabaseException.DataCorruptionException("Failed to read tuple with ID $tupleId.")
            val oldTuple = this.serializer.fromEntry(tupleId, oldTupleRaw)

            /* Delete entry from store. */
            this.store.delete(this.xodusTx, tupleId.toKey())

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val event = DataEvent.Delete(this@DefaultEntity.name, oldTuple.materialize())
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
               this.highAddress == it.highAddress
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
                this.serializer.flush() /* Flush writers. */
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
            val smallest = this.smallestTupleId()
            val next = when {
                smallest == -1L -> 0L
                smallest > 0L -> smallest - 1L
                else -> this.largestTupleId() + 1L
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
                    else -> { /* No op. */ }
                }
            }
        }
    }
}
