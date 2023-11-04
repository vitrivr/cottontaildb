package org.vitrivr.cottontail.dbms.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.*
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.xodus.RefCountedEnvironment
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import java.util.*
import kotlin.concurrent.withLock

/**
 * The default [Entity] implementation based on JetBrains Xodus.
 *
 * @see Entity
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class DefaultEntity(override val name: Name.EntityName, override val parent: DefaultSchema, val handle: UUID) : Entity {

    /** A [DefaultEntity] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [DefaultEntity]. */
    override val version: DBOVersion
        get() = DBOVersion.current()

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [DefaultEntity.Tx] for.
     * @return New [DefaultEntity.Tx]
     */
    override fun newTx(context: QueryContext): EntityTx {
        val tx = this.Tx(context)
        context.transaction.cacheTx(tx) /* Cache this Tx for future use. */
        return tx
    }

    override fun equals(other: Any?): Boolean {
        if (other !is DefaultEntity) return false
        if (other.catalogue != this.catalogue) return false
        return other.name == this.name
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + parent.hashCode()
        return result
    }

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     */
    inner class Tx(context: QueryContext) : AbstractTx(context), EntityTx {
        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /** The [RefCountedEnvironment.Tx] backing this [EntityTx]. */
        private val tx = context.transaction.requestEnvironment(this@DefaultEntity.handle)

        /** The bitmap store that backs this [DefaultEntity]. */
        private val bitmap = this.tx.environment.openBitmap("${this@DefaultEntity.name}", StoreConfig.USE_EXISTING, this.tx.xodusTx)

        /** Map of [Name.ColumnName] to [Column]. */
        private val columns = Object2ObjectLinkedOpenHashMap<Name.ColumnName,ColumnTx<*>>()

        /** Map of [Name.IndexName] to [IndexTx]. */
        private val indexes = Object2ObjectLinkedOpenHashMap<Name.IndexName, Index>()

        init {
            /* Load entity entry.  */
            val entityMetadataStore = EntityMetadata.store(this@DefaultEntity.catalogue, this.context.transaction.xodusTx)
            val entityEntry = EntityMetadata.fromEntry(
                entityMetadataStore.get(this.context.transaction.xodusTx, NameBinding.Entity.toEntry(this@DefaultEntity.name)) ?: throw DatabaseException.DataCorruptionException("Catalogue entry for entity ${this@DefaultEntity.name} is missing.")
            )

            /* Cache this Tx for future use. */
            context.transaction.cacheTx(this)

            /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            val columnMetadataStore = ColumnMetadata.store(this@DefaultEntity.catalogue, this.context.transaction.xodusTx)
            columnMetadataStore.openCursor(this.context.transaction.xodusTx).use {
                if (it.getSearchKeyRange(NameBinding.Entity.toEntry(this@DefaultEntity.name)) != null) {
                    do {
                        val columnName = NameBinding.Column.fromEntry(it.key)
                        if (columnName.entity() != this@DefaultEntity.name) {
                            break
                        }
                        val columnEntry = ColumnMetadata.fromEntry(it.value)
                        val columnDef = ColumnDef(columnName, columnEntry.type, columnEntry.nullable, columnEntry.primary, columnEntry.primary)
                        if (columnDef.type is Types.String || columnDef.type is Types.ByteString) {
                            this.columns[columnName] = VariableLengthColumn(columnDef, this@DefaultEntity)
                        } else {
                            this.columns[columnName] = FixedLengthColumn(columnDef, this@DefaultEntity, columnEntry.compression)
                        }
                    } while (it.next)
                }
            }

            /* Load a map of indexes. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            val indexMetadataStore = IndexMetadata.store(this@DefaultEntity.catalogue, this.context.transaction.xodusTx)
            indexMetadataStore.openCursor(this.context.transaction.xodusTx).use {
                if (it.getSearchKeyRange(NameBinding.Entity.toEntry(this@DefaultEntity.name))  != null) {
                    do {
                        val indexName = NameBinding.Index.fromEntry(it.key)
                        if (indexName.entity() != this@DefaultEntity.name) {
                            break
                        }
                        val indexEntry = IndexMetadata.fromEntry(it.value)
                        this.indexes[indexName] = indexEntry.type.descriptor.open(indexName, this.dbo)
                    } while (it.next)
                }
            }
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
        override fun contains(tupleId: TupleId): Boolean = this.txLatch.withLock {
            this.bitmap.get(this.context.transaction.xodusTx, tupleId)
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
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Tuple = this.txLatch.withLock {
            require(this.bitmap.get(this.tx.xodusTx, tupleId)) { "Tuple with ID $tupleId does not exist." }

            /* Read values from underlying columns. */
            val values = Array(columns.size) {
                val name = columns[it].name
                val columnTx = this.columns[columns[it].name] ?: throw IllegalArgumentException("Column $name does not exist.")
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
        override fun count(): Long = this.txLatch.withLock {
            this.bitmap.count(this.tx.xodusTx)
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun smallestTupleId(): TupleId = this.txLatch.withLock {
            this.bitmap.getFirst(this.tx.xodusTx)
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun largestTupleId(): TupleId = this.txLatch.withLock {
            this.bitmap.getLast(this.tx.xodusTx)
        }

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<ColumnDef<*>> = this.txLatch.withLock {
            this.columns.values.map { it.columnDef }
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): ColumnTx<*> = this.txLatch.withLock {
            val fqn = this@DefaultEntity.name.column(name.simple)
            this.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
        }

        /**
         * Lists all [Name.IndexName] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Name.IndexName> = this.txLatch.withLock {
            this.indexes.keys.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index = this.txLatch.withLock {
            this.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         * @param configuration The [IndexConfig] to initialize the [Index] with.
         * @return Newly created [Index] for use in context of this [Tx]
         */
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: List<Name.ColumnName>, configuration: IndexConfig<*>): Index = this.txLatch.withLock {
            /* Check if entity already exists. */
            require(name.entity() == this@DefaultEntity.name) { "Index $name does not belong to entity! This is a programmer's error!"}

            /* Prepare index entry and persist it. */
            val store = IndexMetadata.store(this@DefaultEntity.catalogue, this.context.transaction.xodusTx)
            val state = if (this.count() == 0L) { IndexState.CLEAN } else { IndexState.DIRTY }
            val indexEntry = IndexMetadata(type, state, columns.map { it.columnName }, configuration)
            if (!store.add(this.context.transaction.xodusTx, NameBinding.Index.toEntry(name), IndexMetadata.toEntry(indexEntry))) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Initialize index store entry. */
            if (!type.descriptor.initialize(name, this.dbo.catalogue, this.context.transaction)) {
                throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to initialize store.")
            }

            /* Try to open index. */
            val ret = type.descriptor.open(name, this@DefaultEntity)
            this.indexes[name] = ret

            /* Signal event to transaction context. */
            this.context.transaction.signalEvent(IndexEvent.Created(name, type))

            /* Return index. */
            return ret
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        override fun dropIndex(name: Name.IndexName) = this.txLatch.withLock {
            /* Fetch index entry and remove it. */
            val store = IndexMetadata.store(this@DefaultEntity.catalogue, this.context.transaction.xodusTx)
            val metadataRaw = store.get(this.context.transaction.xodusTx, NameBinding.Index.toEntry(name)) ?: throw DatabaseException.IndexDoesNotExistException(name)
            val metadata = IndexMetadata.fromEntry(metadataRaw)
            if (!store.delete(this.context.transaction.xodusTx, NameBinding.Index.toEntry(name))) {
                throw DatabaseException.IndexDoesNotExistException(name)
            }

            /* De-initialize the index. */
            if (!metadata.type.descriptor.deinitialize(name, this.dbo.catalogue, this.context.transaction)) {
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to de-initialize store.")
            }

            /* Remove index catalogue entry. */
            this.indexes.remove(name)

            /* Signal event to transaction context. */
            this.context.transaction.signalEvent(IndexEvent.Dropped(name, metadata.type))
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
        override fun cursor(columns: Array<ColumnDef<*>>, rename: Array<Name.ColumnName>): Cursor<Tuple> = this.txLatch.withLock {
            return cursor(columns, this.smallestTupleId()..this.largestTupleId(), rename)
        }

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
        override fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange, rename: Array<Name.ColumnName>) = this.txLatch.withLock {
            DefaultEntityCursor(this, columns, partition, rename)
        }

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
        override fun insert(tuple: Tuple): Tuple = this.txLatch.withLock {
           /* Execute INSERT on column level. */
            val tupleId = nextTupleId()
            val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this.columns.size)
            this.bitmap.set(this.context.transaction.xodusTx, tupleId, true)
            for (column in this.columns.values) {
                /* Make necessary checks for value. */
                val value = when {
                    column.columnDef.autoIncrement -> {
                        /* Obtain sequence and generate next value. */
                        val sequenceName = column.columnDef.name.autoincrement() ?: throw IllegalStateException("")
                        val schemaTx = this@DefaultEntity.parent.newTx(this.context)
                        val sequenceTx = schemaTx.sequenceForName(sequenceName).newTx(this.context)

                        /* Generate and use value. */
                        when (column.columnDef.type) {
                            Types.Int -> sequenceTx.next().asInt()
                            Types.Long -> sequenceTx.next()
                            Types.Uuid -> UuidValue(UUID.randomUUID())
                            else -> throw IllegalStateException("Columns of types ${column.columnDef.type} do not allow for serial values. This is a programmer's error!")
                        }
                    }
                    column.columnDef.nullable -> tuple[column.columnDef]
                    else -> tuple[column.columnDef] ?: throw DatabaseException.ValidationException("Cannot INSERT a NULL value into column ${column.columnDef}.")
                }

                /* Record and perform insert. */
                inserts[column.columnDef] = value
                if (value != null) {
                    (column as ColumnTx<Value>).write(tupleId, value)
                }
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes. */
            val event = DataEvent.Insert(this@DefaultEntity.name, tupleId, inserts)
            for (index in this.indexes.values) {
                index.newTx(this.context).insert(event)
            }

            /* Signal event to transaction context. */
            this.context.transaction.signalEvent(event)

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
        override fun update(tuple: Tuple) = this.txLatch.withLock {
            /* Execute UPDATE on column level. */
            val updates = Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?, Value?>>(tuple.columns.size)
            for (def in tuple.columns) {
                val columnTx = (this.columns[def.name] as? ColumnTx<Value>) ?: throw DatabaseException.ColumnDoesNotExistException(def.name)
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
                index.newTx(this.context).update(event)
            }

            /* Signal event to transaction context. */
            this.context.transaction.signalEvent(event)
        }

        /**
         * Deletes the entry with the provided [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.txLatch.withLock {
            /* Perform DELETE on column level. */
            val deleted = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.columns.size)
            for (column in this.columns.values) {
                deleted[column.columnDef] = column.delete(tupleId)
            }

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val event = DataEvent.Delete(this@DefaultEntity.name, tupleId, deleted)
            for (index in this.indexes.values) {
                index.newTx(this.context).delete(event)
            }

            /* Signal event to transaction context. */
            this.context.transaction.signalEvent(event)
        }

        /**
         * Obtains the next free [TupleId] based on the entity bitmap.
         *
         * @return Next [TupleId] for insert.
         */
        private fun nextTupleId(): TupleId = this.txLatch.withLock {
            val smallest = this.bitmap.getFirst(this.context.transaction.xodusTx)
            val next = when {
                smallest == -1L -> 0L
                smallest > 0L -> smallest - 1L
                else -> this.bitmap.getLast(this.context.transaction.xodusTx) + 1L
            }
            return@withLock next
        }
    }
}
