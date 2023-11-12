package org.vitrivr.cottontail.dbms.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue.Companion.INDEX_METADATA_STORE_NAME
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.*
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.EntityEvent
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import java.util.concurrent.locks.ReentrantLock
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
class DefaultEntity(override val name: Name.EntityName, override val parent: DefaultSchema, private val environment: Environment) : Entity {

    /** A [DefaultEntity] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [DefaultEntity]. */
    override val version: DBOVersion
        get() = DBOVersion.current()

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [QueryContext].
     *
     * @param parent The [SchemaTx] to create the [DefaultEntity.Tx] for.
     * @return New [DefaultEntity.Tx]
     */
    override fun newTx(parent: SchemaTx): DefaultEntity.Tx {
        require(parent is DefaultSchema.Tx) { "VariableLengthColumn can only be used with DefaultEntity.Tx" }
        return this.Tx(parent)
    }

    /**
     * Compares this [DefaultEntity] to another object.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is DefaultEntity) return false
        if (other.catalogue != this.catalogue) return false
        return other.name == this.name
    }

    /**
     * Hash code for this [DefaultEntity].
     */
    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + parent.hashCode()
        return result
    }

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     */
    inner class Tx(parent: DefaultSchema.Tx): EntityTx, org.vitrivr.cottontail.dbms.general.Tx.Commitable {

        /** Reference to the Cottontail DB [Transaction] object. */
        override val transaction: Transaction = parent.transaction

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /** The entity-level Xodus [jetbrains.exodus.env.Transaction] used by this [Tx]. */
        internal val xodusTx: jetbrains.exodus.env.Transaction = this@DefaultEntity.environment.beginTransaction()

        /** Map of [Name.ColumnName] to [Column]. */
        private val columns = Object2ObjectLinkedOpenHashMap<Name.ColumnName,Column<*>>()

        /** Map of [Name.IndexName] to [IndexTx]. */
        private val indexes = Object2ObjectLinkedOpenHashMap<Name.IndexName, Index>()

        /** The bitmap store that backs this [DefaultEntity]. */
        private var bitmap = this.xodusTx.environment.openBitmap("${this@DefaultEntity.name}", StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

        /** A [ReentrantLock] that synchronises access to this [Tx]'s methods. */
        private val txLatch = ReentrantLock()

        init {
            /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            val columnMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.COLUMN_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for column metadata.")
            columnMetadataStore.openCursor(this.xodusTx).use {
                if (it.getSearchKeyRange(NameBinding.Entity.toEntry(this@DefaultEntity.name)) != null) {
                    do {
                        val columnName = NameBinding.Column.fromEntry(it.key)
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
            val indexMetadataStore = this.xodusTx.environment.openStore(INDEX_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")
            indexMetadataStore.openCursor(this.xodusTx).use {
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
            this.bitmap.get(this.xodusTx, tupleId)
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
            require(this.bitmap.get(this.xodusTx, tupleId)) { "Tuple with ID $tupleId does not exist." }

            /* Read values from underlying columns. */
            val values = Array(columns.size) {
                val columnTx = this.transaction.columnTx(columns[it].name, AccessMode.READ)
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
            this.bitmap.count(this.xodusTx)
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun smallestTupleId(): TupleId = this.txLatch.withLock {
            this.bitmap.getFirst(this.xodusTx)
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun largestTupleId(): TupleId = this.txLatch.withLock {
            this.bitmap.getLast(this.xodusTx)
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
        override fun columnForName(name: Name.ColumnName): Column<*> = this.txLatch.withLock {
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
            val store = this@DefaultEntity.environment.openStore(INDEX_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")
            val state = if (this.count() == 0L) { IndexState.CLEAN } else { IndexState.STALE }
            val indexEntry = IndexMetadata(type, state, columns.map { it.columnName }, configuration)
            if (!store.add(this.xodusTx, NameBinding.Index.toEntry(name), IndexMetadata.toEntry(indexEntry))) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Try to open index. */
            val ret = type.descriptor.open(name, this@DefaultEntity)
            this.indexes[name] = ret

            /* Signal event to transaction context. */
            this.transaction.signalEvent(IndexEvent.Created(name, type))

            /* Return index. */
            return ret
        }

        /**
         * Truncates (i.e., clears) the [DefaultEntity] backed by this [EntityTx].
         */
        override fun truncate() = this.txLatch.withLock {
            /* Truncate all columns. */
            val truncated = mutableListOf<ColumnDef<*>>()
            for (column in this.columns.values) {
                val columnTx = this.transaction.columnTx(column.name, AccessMode.WRITE)
                columnTx.truncate()
                truncated.add(column.columnDef)
            }

            /* Truncate all indexes. */
            for (index in this.indexes.values) {
                val indexTx = this.transaction.indexTx(index.name, AccessMode.WRITE)
                indexTx.truncate()
            }

            /* Drop store for entity (bitmap). */
            this.xodusTx.environment.truncateStore("$name#bitmap", this.xodusTx)
            this.bitmap = this.xodusTx.environment.openBitmap("${this@DefaultEntity.name}", StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

            /* Create Event and notify observers */
            val event = EntityEvent.Truncate(this@DefaultEntity.name, truncated.toTypedArray())
            this.transaction.signalEvent(event)
        }

        /**
         * Drops the [DefaultEntity] backed by this [EntityTx].
         */
        override fun drop() = this.txLatch.withLock {
            /* Drop all columns. */
            val dropped = mutableListOf<ColumnDef<*>>()
            for (column in this.columns.values) {
                val columnTx = this.transaction.columnTx(column.name, AccessMode.WRITE)
                columnTx.drop()
                dropped.add(column.columnDef)
            }

            /* Drop all indexes. */
            for (index in this.indexes.values) {
                val indexTx = this.transaction.indexTx(index.name, AccessMode.WRITE)
                indexTx.drop()
            }

            /* Drop store for entity (bitmap). */
            this.xodusTx.environment.removeStore("$name#bitmap", this.xodusTx)

            /* Now remove all catalogue entries related to entity. */
            val entityMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.ENTITY_METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
            if (!entityMetadataStore.delete(this.xodusTx, NameBinding.Entity.toEntry(name))) {
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete catalogue entry.")
            }

            /* Create Event and notify observers */
            val event = EntityEvent.Drop(this@DefaultEntity.name, dropped.toTypedArray())
            this.transaction.signalEvent(event)
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
            this.bitmap.set(this.xodusTx, tupleId, true)
            for (column in this.columns.values) {
                val columnTx = this.transaction.columnTx(column.name, AccessMode.READ) as ColumnTx<Value>

                /* Make necessary checks for value. */
                val value = when {
                    column.columnDef.autoIncrement -> columnTx.nextAutoValue()
                    column.columnDef.nullable -> tuple[column.columnDef]
                    else -> tuple[column.columnDef] ?: throw DatabaseException.ValidationException("Cannot INSERT a NULL value into column ${column.columnDef}.")
                }

                /* Record and perform insert. */
                inserts[column.columnDef] = value
                if (value != null) {
                    columnTx.write(tupleId, value)
                }
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes. */
            val event = DataEvent.Insert(this@DefaultEntity.name, tupleId, inserts)
            for (index in this.indexes.values) {
                val indexTx = this.transaction.indexTx(index.name, AccessMode.READ)
                indexTx.insert(event)
            }

            /* Signal event to transaction context. */
            this.transaction.signalEvent(event)

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
                val columnTx = this.transaction.columnTx(def.name, AccessMode.READ) as ColumnTx<Value>
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
                val indexTx = this.transaction.indexTx(index.name, AccessMode.READ)
                indexTx.update(event)
            }

            /* Signal event to transaction context. */
            this.transaction.signalEvent(event)
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
                val columnTx = this.transaction.columnTx(column.name, AccessMode.READ)
                deleted[column.columnDef] = columnTx.delete(tupleId)
            }

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val event = DataEvent.Delete(this@DefaultEntity.name, tupleId, deleted)
            for (index in this.indexes.values) {
                val indexTx = this.transaction.indexTx(index.name, AccessMode.READ)
                indexTx.delete(event)
            }

            /* Signal event to transaction context. */
            this.transaction.signalEvent(event)
        }

        /**
         * Commits the local Xodus [jetbrains.exodus.env.Transaction] and persists all changes.
         */
        override fun commit() {
            this.xodusTx.commit()
        }

        /**
         * Aborts the local Xodus [jetbrains.exodus.env.Transaction] and persists all changes.
         */
        override fun rollback() {
            this.xodusTx.abort()
        }

        /**
         * Obtains the next free [TupleId] based on the entity bitmap.
         *
         * @return Next [TupleId] for insert.
         */
        private fun nextTupleId(): TupleId = this.txLatch.withLock {
            val smallest = this.bitmap.getFirst(this.xodusTx)
            val next = when {
                smallest == -1L -> 0L
                smallest > 0L -> smallest - 1L
                else -> this.bitmap.getLast(this.xodusTx) + 1L
            }
            return@withLock next
        }
    }
}
