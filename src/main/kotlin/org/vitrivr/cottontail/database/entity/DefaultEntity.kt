package org.vitrivr.cottontail.database.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.*
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.io.FileUtilities
import java.io.IOException
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single entity in the Cottontail DB data model. An [DefaultEntity] has name that must remain unique within a [DefaultSchema].
 * The [DefaultEntity] contains one to many [Column]s holding the actual data. Hence, it can be seen as a table containing tuples.
 *
 * Calling the default constructor for [DefaultEntity] opens that [DefaultEntity]. It can only be opened once due to file locks and it
 * will remain open until the [Entity.close()] method is called.
 *
 * @see DefaultSchema
 * @see Column
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DefaultEntity(override val path: Path, override val parent: DefaultSchema): Entity {
    /**
     * Companion object of the [DefaultEntity]
     */
    companion object {
        /** Filename for the [DefaultEntity] catalogue.  */
        const val CATALOGUE_FILE = "index.db"

        /** Filename for the [DefaultEntity] catalogue.  */
        const val ENTITY_HEADER_FIELD = "cdb_entity_header"

        /** Filename for the [DefaultEntity] catalogue.  */
        const val ENTITY_COUNT_FIELD = "cdb_entity_count"

        /** Filename for the [DefaultEntity] catalogue.  */
        const val ENTITY_MAX_FIELD = "cdb_entity_maxtid"
    }

    /** Internal reference to the [StoreWAL] underpinning this [DefaultEntity]. */
    private val store: DB = try {
        this.parent.parent.config.mapdb.db(this.path.resolve(CATALOGUE_FILE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity at $path: ${e.message}'.")
    }

    /** The [EntityHeader] of this [DefaultEntity]. */
    private val header =
        this.store.atomicVar(ENTITY_HEADER_FIELD, EntityHeader.Serializer).createOrOpen()

    /** The number of entries in this [DefaultEntity]. */
    private val countField = this.store.atomicLong(ENTITY_COUNT_FIELD).createOrOpen()

    /** The maximum [TupleId] in this [DefaultEntity]. */
    private val maxTupleIdField = this.store.atomicLong(ENTITY_MAX_FIELD).createOrOpen()

    /** The [Name.EntityName] of this [DefaultEntity]. */
    override val name: Name.EntityName = this.parent.name.entity(this.header.get().name)

    /** An internal lock that is used to synchronize access to this [DefaultEntity] in presence of ongoing [Tx]. */
    private val closeLock = StampedLock()

    /** List of all the [Column]s associated with this [DefaultEntity]; Iteration order of entries as defined in schema! */
    private val columns: MutableMap<Name.ColumnName, Column<*>> = Object2ObjectLinkedOpenHashMap()

    /** List of all the [Index]es associated with this [DefaultEntity]. */
    private val indexes: MutableMap<Name.IndexName, Index> = Object2ObjectOpenHashMap()

    /** The [DBOVersion] of this [DefaultEntity]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Number of [Column]s in this [DefaultEntity]. */
    override val numberOfColumns: Int
        get() = this.columns.size

    /** Number of entries in this [DefaultEntity]. This is a snapshot and may change immediately. */
    override val numberOfRows: Long
        get() = this.countField.get()

    /** Estimated maximum [TupleId]s for this [DefaultEntity].  This is a snapshot and may change immediately. */
    override val maxTupleId: TupleId
        get() = this.maxTupleIdField.get()

    /**
     * Status indicating whether this [DefaultEntity] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    init {
        /** Load and initialize the columns. */
        val header = this.header.get()
        header.columns.map {
            val columnName = this.name.column(it.name)
            this.columns[columnName] = it.type.open(it.path, this)
        }

        /** Load and initialize the indexes. */
        header.indexes.forEach {
            val indexName = this.name.index(it.name)
            indexes[indexName] = it.type.open(it.path, this)
        }
    }

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultEntity.Tx] for.
     * @return New [DefaultEntity.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes the [DefaultEntity].
     *
     * Closing an [DefaultEntity] is a delicate matter since ongoing [DefaultEntity.Tx] objects as well
     * as all involved [Column]s are involved.Therefore, access to the method is mediated by an
     * global [DefaultEntity] wide lock.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.columns.values.forEach { it.close() }
            this.store.close()
            this.closed = true
        }
    }

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [DefaultEntity] soft-reference.
     */
    @Synchronized
    protected fun finalize() {
        if (!this.closed) {
            this.close()
        }
    }

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), EntityTx {

        /** Obtains a global non-exclusive lock on [DefaultEntity]. Prevents [DefaultEntity] from being closed. */
        private val closeStamp = this@DefaultEntity.closeLock.readLock()

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /** [TxSnapshot] of this [EntityTx] */
        override val snapshot = object : EntityTxSnapshot {
            override var delta: Long = 0L
            override val indexes =
                Object2ObjectOpenHashMap<Name.IndexName, Index>(this@DefaultEntity.indexes)

            /**
             * Commits the [EntityTx] and integrates all changes made through it into the [DefaultEntity].
             */
            override fun commit() {
                /* Materialize created indexes. */
                this.indexes.forEach {
                    if (!this@DefaultEntity.indexes.contains(it.key)) {
                        this@DefaultEntity.indexes[it.key] = it.value
                    }
                }

                /* Materialize dropped indexes. */
                this@DefaultEntity.indexes.forEach { (k, v) ->
                    if (!this.indexes.contains(k)) {
                        v.close()
                        FileUtilities.deleteRecursively(v.path)
                    }
                }

                /* Update and persist header + commit store. */
                val oldHeader = this@DefaultEntity.header.get()
                this@DefaultEntity.header.compareAndSet(
                    oldHeader,
                    oldHeader.copy(modified = System.currentTimeMillis())
                )
                this@DefaultEntity.countField.addAndGet(this.delta)
                this@DefaultEntity.maxTupleIdField.set(this@DefaultEntity.columns.values.first().maxTupleId)
                this@DefaultEntity.store.commit()
            }

            /**
             * Rolls back the [EntityTx] and integrates all changes made through it into the [DefaultEntity].
             */
            override fun rollback() {
                /* Delete newly created indexes. */
                this.indexes.forEach {
                    if (!this@DefaultEntity.indexes.contains(it.key)) {
                        it.value.close()
                        FileUtilities.deleteRecursively(it.value.path)
                    }
                }
                this@DefaultEntity.store.rollback()
            }
        }

        /** Tries to acquire a global read-lock on this entity. */
        init {
            if (this@DefaultEntity.closed) {
                throw TxException.TxDBOClosedException(this.context.txId)
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Record]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @param columns The [ColumnDef]s that should be read.
         * @return The desired [Record].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record = this.withReadLock {
            /* Read values from underlying columns. */
            val values = columns.map {
                val column = this@DefaultEntity.columns[it.name]
                        ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@DefaultEntity.name}.")
                (this.context.getTx(column) as ColumnTx<*>).read(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        override fun count(): Long = this.withReadLock {
            return this@DefaultEntity.countField.get()
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun maxTupleId(): TupleId = this.withReadLock {
            return this@DefaultEntity.maxTupleIdField.get()
        }

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<Column<*>> = this.withReadLock {
            return this@DefaultEntity.columns.values.toList()
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> = this.withReadLock {
            if (!name.wildcard) {
                this@DefaultEntity.columns[name] ?: throw DatabaseException.ColumnDoesNotExistException(
                    name
                )
            } else {
                val fqn = this@DefaultEntity.name.column(name.simple)
                this@DefaultEntity.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
            }
        }

        /**
         * Lists all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Index> = this.withReadLock {
            this.snapshot.indexes.values.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index = this.withReadLock {
            this.snapshot.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         */
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: Array<ColumnDef<*>>, params: Map<String, String>): Index = this.withWriteLock {
            if (this.snapshot.indexes.containsKey(name)) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Creates and opens the index and adds it to snapshot. */
            val newIndex = type.create(
                this@DefaultEntity.path.resolve("${name.simple}.db"),
                this.dbo,
                name,
                columns,
                params
            )
            this.snapshot.indexes[newIndex.name] = newIndex

            try {
                /* Update header. */
                val oldHeader = this@DefaultEntity.header.get()
                val newHeader = oldHeader.copy(
                    indexes = (oldHeader.indexes + EntityHeader.IndexRef(
                        newIndex.name.simple,
                        newIndex.type,
                        newIndex.path
                    ))
                )
                this@DefaultEntity.header.compareAndSet(oldHeader, newHeader)
                return newIndex
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to create index '$name' due to a storage exception: ${e.message}")
            }
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        override fun dropIndex(name: Name.IndexName) = this.withWriteLock {
            /* Obtain index and acquire exclusive lock on it. */
            val index = this.snapshot.indexes.remove(name)
                ?: throw DatabaseException.IndexDoesNotExistException(name)
            if (this.context.lockOn(index) == LockMode.NO_LOCK) {
                this.context.requestLock(index, LockMode.EXCLUSIVE)
            }

            try {
                /* Update header. */
                val oldHeader = this@DefaultEntity.header.get()
                this@DefaultEntity.header.set(oldHeader.copy(indexes = oldHeader.indexes.filter { it.name != index.name.simple }))
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to drop index '$name' due to a storage exception: ${e.message}")
            } catch (e: IOException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to drop index '$name' due to an IO exception: ${e.message}")
            }
        }


        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns
         * all [TupleId]s contained within the surrounding [DefaultEntity].
         *
         * <strong>Important:</strong> It remains to the caller to close the [Iterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         *
         * @return [Iterator]
         */
        override fun scan(columns: Array<ColumnDef<*>>): Iterator<Record> =
            scan(columns, 1L..this.maxTupleId())

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns all [TupleId]s
         * contained within the surrounding [DefaultEntity] and a certain range.
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param range The [LongRange] that should be scanned.
         *
         * @return [Iterator]
         */
        override fun scan(columns: Array<ColumnDef<*>>, range: LongRange) =
            object : Iterator<Record> {

                /** The wrapped [Iterator] of the first (primary) column. */
                private val wrapped = this@Tx.withReadLock {
                    (this@Tx.context.getTx(this@DefaultEntity.columns.values.first()) as ColumnTx<*>).scan(
                        range
                    )
                }

                /**
                 * Returns the next element in the iteration.
                 */
                override fun next(): Record {
                    /* Read values from underlying columns. */
                    val tupleId = this.wrapped.next()
                    val values = columns.map {
                        val column = this@DefaultEntity.columns[it.name]
                            ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@DefaultEntity.name}.")
                    (this@Tx.context.getTx(column) as ColumnTx<*>).read(tupleId)
                }.toTypedArray()

                /* Return value of all the desired columns. */
                return StandaloneRecord(tupleId, columns, values)
            }

                /**
                 * Returns `true` if the iteration has more elements.
                 */
                override fun hasNext(): Boolean = this.wrapped.hasNext()
        }

        /**
         * Insert the provided [Record]. This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         *
         * @throws TxException If some of the [Tx] on [Column] or [Index] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun insert(record: Record): TupleId? = this.withWriteLock {
            try {
                var lastTupleId: TupleId? = null
                val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this@DefaultEntity.columns.size)
                this@DefaultEntity.columns.values.forEach {
                    val tx = this.context.getTx(it) as ColumnTx<Value>
                    val value = record[it.columnDef]
                    val tupleId = tx.insert(value)
                    if (lastTupleId != tupleId && lastTupleId != null) {
                        throw DatabaseException.DataCorruptionException("Entity '${this@DefaultEntity.name}' is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastTupleId = tupleId
                    inserts[it.columnDef] = value
                }

                if (lastTupleId != null) {
                    /* Increment delta. */
                    this.snapshot.delta += 1

                    /* Issue DataChangeEvent.InsertDataChange event & update indexes. */
                    /* ToDo: System wide event bus for [DataChangeEvent]. */
                    val event = DataChangeEvent.InsertDataChangeEvent(
                        this@DefaultEntity.name,
                        lastTupleId!!,
                        inserts
                    )
                    this@DefaultEntity.indexes.values.forEach {
                        (this.context.getTx(it) as IndexTx).update(event)
                    }
                }

                return lastTupleId
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Inserting record failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [DefaultEntity] will cause an error! This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun update(record: Record) = this.withWriteLock {
            try {
                val updates = Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?,Value?>>(record.columns.size)
                record.columns.forEach { def ->
                    val column = this@DefaultEntity.columns[def.name] ?: throw IllegalArgumentException("Column $def does not exist on entity ${this@DefaultEntity.name}.")
                    val value = record[def]
                    val columnTx = (this.context.getTx(column) as ColumnTx<Value>)
                    updates[def] = Pair(columnTx.update(record.tupleId, value), value) /* Map: ColumnDef -> Pair[Old, New]. */
                }

                /* Issue DataChangeEvent.UpdateDataChangeEvent event & update indexes. */
                /* ToDo: System wide event bus for [DataChangeEvent]. */
                val event = DataChangeEvent.UpdateDataChangeEvent(this@DefaultEntity.name, record.tupleId, updates)
                this@DefaultEntity.indexes.values.forEach {
                    (this.context.getTx(it) as IndexTx).update(event)
                }
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Updating record ${record.tupleId} failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Deletes the entry with the provided [TupleId]. This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY]
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.withWriteLock {
            try {
                /* Perform delete on each column. */
                val deleted =
                    Object2ObjectArrayMap<ColumnDef<*>, Value?>(this@DefaultEntity.columns.size)
                this@DefaultEntity.columns.values.map {
                    deleted[it.columnDef] = (this.context.getTx(it) as ColumnTx<*>).delete(tupleId)
                }

                /* Decrement delta. */
                this.snapshot.delta -= 1

                /* Issue DataChangeEvent.DeleteDataChangeEvent event & update indexes. */
                /* ToDo: System wide event bus for [DataChangeEvent]. */
                val event =
                    DataChangeEvent.DeleteDataChangeEvent(this@DefaultEntity.name, tupleId, deleted)
                this@DefaultEntity.indexes.values.forEach {
                    (this.context.getTx(it) as IndexTx).update(event)
                }
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Deleting record $tupleId failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Releases the [closeLock] on the [DefaultEntity].
         */
        override fun cleanup() {
            this@DefaultEntity.closeLock.unlockRead(this.closeStamp)
        }
    }
}
