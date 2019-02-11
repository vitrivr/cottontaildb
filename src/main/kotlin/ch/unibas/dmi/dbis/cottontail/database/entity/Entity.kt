package ch.unibas.dmi.dbis.cottontail.database.entity

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnTransaction
import ch.unibas.dmi.dbis.cottontail.database.column.mapdb.MapDBColumn
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.basics.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.basics.Tuple

import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.TransactionException

import org.mapdb.*
import org.mapdb.volume.MappedFileVol
import java.nio.file.Path
import java.util.*

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Represents a single entity in the Cottontail DB data model. An [Entity] has name that must remain unique within a [Schema].
 * The [Entity] contains one to many [Column]s holding the actual data. Hence, it can be seen as a table containing tuples.
 *
 * Calling the default constructor for [Entity] opens that [Entity]. It can only be opened once due to file locks and it
 * will remain open until the [Entity.close()] method is called.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
internal class Entity(override val name: String, schema: Schema): DBO {
    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = schema.path.resolve("entity_$name")

    /** The parent [DBO], which is the [Schema] in case of an [Entity]. */
    override val parent: Schema? = schema

    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY , fileLockWait = this.parent!!.parent.config.lockTimeout)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity '$fqn': ${e.message}'.")
    }

    /** The header of this [Entity]. */
    private val header: EntityHeader
        get() = this.store.get(HEADER_RECORD_ID, EntityHeaderSerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open header of entity '$fqn'!'")

    /** A internal lock that is used to synchronize [Entity.Tx] affecting this [Entity]. */
    private val txLock = ReentrantReadWriteLock()

    /** A internal lock that is used to synchronize closing of an [Entity] with running [Entity.Tx]. */
    private val globalLock = ReentrantReadWriteLock()

    /** List of all the [Column]s associated with this [Entity]. */
    private val columns: List<Column<*>> = header.columns.map {
        MapDBColumn<Any>(this.store.get(it, Serializer.STRING) ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$fqn': Could not read column at index $it!'"), this)
    }

    /**
     * Status indicating whether this [Entity] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Number of [Column]s held by this [Entity].
     */
    val columnCount: Int
        get() = this.header.columns.size

    /**
     * Returns the [ColumnDef] for the specified name.
     *
     * @param name The name of the [Column].
     * @return [ColumnDef] of the [Column].
     */
    fun columnForName(name: String): ColumnDef<*>? = this.columns.find { it.name == name }?.columnDef

    /**
     * Closes the [Entity]. Closing an [Entity] is a delicate matter since ongoing [Entity.Tx] objects as well as all involved [Column]s are involved.
     * Therefore, access to the method is mediated by an global [Entity] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.columns.forEach { it.close() }
        this.store.close()
        this.closed = true
    }

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [Entity] soft-reference.
     */
    @Synchronized
    protected fun finalize() {
        if (!this.closed) {
            this.close()
        }
    }

    /**
     * Companion object of the [Entity]
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        internal const val FILE_CATALOGUE = "index.db"

        /** Filename for the [Entity] catalogue.  */
        internal const val HEADER_RECORD_ID = 1L
    }

    /**
     * A [Tx] that affects this [Entity].
     *
     * Opening such a [Tx] will spawn a associated [Column.Tx] for every [Column] associated with this [Entity].
     */
    inner class Tx(override val readonly: Boolean, override val tid: UUID = UUID.randomUUID()) : Transaction {
        /** List of [Column.Tx] associated with this [Entity.Tx]. */
        private val transactions: Map<ColumnDef<*>, ColumnTransaction<*>> = mapOf(* this@Entity.columns.map { Pair(ColumnDef(it.name, it.type, it.size), it.newTransaction(readonly, tid)) }.toTypedArray())

        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** Tries to acquire a global read-lock on this [Entity]. */
        init {
            if (this@Entity.closed) {
                throw TransactionException.TransactionDBOClosedException(tid)
            }
            this@Entity.globalLock.readLock().lock()
        }

        /**
         * Commits all changes made through this [Entity.Tx] since the last commit or rollback.
         */
        @Synchronized
        override fun commit() {
            if (this.status == TransactionStatus.DIRTY) {
                this.transactions.values.forEach { it.commit() }
                this@Entity.store.commit()
                this@Entity.txLock.writeLock().unlock()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Entity.Tx] back to the last commit.
         */
        @Synchronized
        override fun rollback() {
            if (this.status == TransactionStatus.DIRTY) {
                this.transactions.values.forEach { it.rollback() }
                this@Entity.store.rollback()
                this@Entity.txLock.writeLock().unlock()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Entity.Tx] and thereby releases all the [Column.Tx] and the global lock. Closed [Entity.Tx] cannot be used anymore!
         */
        @Synchronized
        override fun close() {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY) {
                    this@Entity.store.rollback()
                    this@Entity.txLock.writeLock().unlock()
                }
                this.status = TransactionStatus.CLOSED
                this@Entity.globalLock.readLock().unlock()
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Tuple]
         *
         * @param tupleId The ID of the desired entry.
         * @param columns The the [Column]s that should be read.
         * @return The desired [Tuple].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        fun read(tupleId: Long, vararg columns: ColumnDef<*>): Record = this@Entity.txLock.read {
            checkValidOrThrow()
            checkValidTupleId(tupleId)
            checkColumnsExist(*columns)

            /* Return value of all the desired columns. */
            StandaloneRecord(tupleId, *columns).assign(*columns.map { transactions.getValue(it).read(tupleId) }.toTypedArray())
        }

        /**
         * Reads the specified values of one or many [Column]s and returns them as a [Recordset]
         *
         * @param tupleId The ID of the desired entry.
         * @param columns The the [Column]s that should be read.
         * @return The resulting [Recordset].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        fun readMany(tupleIds: Collection<Long>, vararg columns: ColumnDef<*>): Recordset = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(*columns)
            val dataset = Recordset(*columns)
            tupleIds.forEach {tid ->
                checkValidTupleId(tid)
                dataset.addRow(tid, *columns.map { transactions.getValue(it).read(tid) }.toTypedArray())
            }
            dataset
        }

        /**
         * Reads all values of one or many [Column]s and returns them as a [Recordset].
         *
         * @param columns The the [Column]s that should be read.
         * @return The resulting [Recordset].
         */
        fun readAll(vararg columns: ColumnDef<*>): Recordset = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(*columns)
            val dataset = Recordset(*columns)
            val data = Array<Any?>(columns.size, {})
            this.transactions.getValue(columns[0]).forEach { id, value ->
                data[0] = value
                for (i in 1 until columns.size) {
                    data[i] = this.transactions.getValue(columns[i]).read(id)

                }
                dataset.addRow(id, *data)
            }
            return dataset
        }

        /**
         * Reads all values of one or many [Column]s and returns those that match the provided predicate as a [Recordset].
         *
         * @param predicate The [BooleanPredicate] to apply. Only columns contained in that [BooleanPredicate] will be read.
         * @return The resulting [Recordset].
         */
        fun filter(predicate: BooleanPredicate): Recordset = this@Entity.txLock.read {
            checkValidOrThrow()
            val columns = predicate.columns.toTypedArray()
            checkColumnsExist(*columns)
            val dataset = Recordset(*columns)
            val data = Array<Any?>(columns.size, {})
            this.transactions.getValue(columns[0]).forEach { id, value ->
                data[0] = value
                for (i in 1 until columns.size) {
                    data[i] = this.transactions.getValue(columns[i]).read(id)
                }
                dataset.addRowIf(id, predicate, *data)
            }
            dataset
        }

        /**
         * Returns the number of entries in this [Entity].
         *
         * @return The number of entries in this [Entity].
         */
        fun count(): Long = this@Entity.txLock.read {
            checkValidOrThrow()
            return this@Entity.header.size
        }

        /**
         * Applies the provided mapping function on each [Tuple] found in this [Entity], returning a collection of the desired output values.
         *
         * @param action The mapping that should be applied to each [Tuple].
         * @param columns The list of [ColumnSpec]s that identify the [Column]s that should be included in the [Tuple].
         *
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        fun <R> map(action: (Record) -> R, vararg columns: ColumnDef<*>) = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(*columns)
        }

        /**
         * Applies the provided mapping function on each value found in this [Column], returning a collection of the desired output values.
         *
         * @param action The mapping that should be applied to each [Column] entry.
         * @param column The [ColumnSpec] that identifies the [Column] that should be mapped.
         *
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        fun <T: Any,R> mapColumn(action: (T?) -> R?, column: ColumnDef<T>): Collection<Tuple<R?>> = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(column)
            return (this.transactions.getValue(column) as ColumnTransaction<T>).map(action)
        }

        /**
         * Applies the provided predicate function on each value found in the specified [Column], returning a collection
         * of output values that pass the predicate's test (i.e. return true)
         *
         * @param predicate The tasks that should be applied.
         * @param column The [ColumnSpec] that identifies the [Column] that should be filtered.
         *
         * @return A filtered collection of [Column] values that passed the test.
         */
        fun <T: Any> filterColumn(predicate: (T?) -> Boolean, column: ColumnDef<T>): Collection<Tuple<T?>> = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(column)
            return (this.transactions.getValue(column) as ColumnTransaction<T>).filter(predicate)
        }

        /**
         * Applies the provided function to each entry found in this [Column]. The provided function cannot not change
         * the data stored in the [Column]!
         *
         * @param action The function to apply to each [Column] entry.
         * @param column The [ColumnSpec] that identifies the [Column] that should be processed.
         */
        fun <T: Any> forEachColumn(action: (Long,T?) -> Unit, column: ColumnDef<T>) = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(column)
            (this.transactions.getValue(column) as ColumnTransaction<T>).forEach(action)
        }

        /**
         * Applies the provided function to each entry found in this [Column] and does so in a parallel fashion using co-routines, which
         * each operate on a disjoint partition of the data. The provided function cannot not change the data stored in the [Column]!
         *
         * It is up to the developer of the function to make sure, that the provided function operates in a thread-safe manner.
         *
         * @param action The function to apply to each [Column] entry.
         * @param column The [ColumnSpec] that identifies the [Column] that should be processed.
         * @param parallelism The desired amount of parallelism (i.e. the number of co-routines to spawn).
         */
        fun <T: Any> parallelForEachColumn(action: (Long,T?) -> Unit, column: ColumnDef<T>, parallelism: Short = 2) = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(column)
            (this.transactions.getValue(column) as ColumnTransaction<T>).parallelForEach(action, parallelism)
        }

        /**
         * Attempts to insert the provided [Tuple] into the [Entity]. Columns specified in the [Tuple] that are not part
         * of the [Entity] will cause an error!
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun insert(record: Record): Long? {
            checkColumnsExist(*record.columns) /* Perform sanity check on columns before locking. */
            acquireWriteLock()
            try {
                var lastRecId: Long? = null
                for ((column,tx) in this.transactions) {
                    val recId = (tx as ColumnTransaction<Any>).insert(record[column])
                    if (lastRecId != recId && lastRecId != null) {
                        throw DatabaseException.DataCorruptionException("Entity ${this@Entity.fqn} is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastRecId = recId
                }

                /* Update the header of this entity. */
                if (lastRecId != null) {
                    val header = this@Entity.header
                    header.size += 1
                    header.modified = System.currentTimeMillis()
                    this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
                }

                return lastRecId
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            }
        }

        /**
         * Attempts to insert the provided [Tuple]s into the [Entity]. Columns specified in the [Tuple] that are not part
         * of the [Entity] will cause an error!
         *
         * @param tuples The [Tuple] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun insertAll(tuples: Collection<Record>): Collection<Long?> {
            tuples.forEach { checkColumnsExist(*it.columns) }  /* Perform sanity check on columns before locking. */
            acquireWriteLock()
            try {
                /* Perform delete on each column. */
                val tuplesIds = tuples.map {
                    var lastRecId: Long? = null
                    for ((column,tx) in this.transactions) {
                        val recId = (tx as ColumnTransaction<Any>).insert(it[column])
                        if (lastRecId != recId && lastRecId != null) {
                            throw DatabaseException.DataCorruptionException("Entity ${this@Entity.fqn} is corrupt. Insert did not yield same record ID for all columns involved!")
                        }
                        lastRecId = recId
                    }
                    lastRecId
                }

                /* Update header. */
                val header = this@Entity.header
                header.size += tuples.size
                header.modified = System.currentTimeMillis()
                store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)

                return tuplesIds
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            }
        }

        /**
         * Attempts to delete the provided [Tuple] from the [Entity]. This tasks will set this [Entity.Tx] to
         * [TransactionStatus.DIRTY] and acquire a [Entity]-wide write lock until the [Entity.Tx] either commit
         * or rollback is issued.
         *
         * @param tupleId The ID of the [Tuple] that should be deleted.
         *
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun delete(tupleId: Long) {
            acquireWriteLock()
            try {
                /* Perform delete on each column. */
                this.transactions.values.forEach { it.delete(tupleId) }

                /* Update header. */
                val header = this@Entity.header
                header.size -= 1
                header.modified = System.currentTimeMillis()
                store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            }
        }

        /**
         * Attempts to delete all the provided [Tuple] from the [Entity]. This tasks will set this [Entity.Tx] to
         * [TransactionStatus.DIRTY] and acquire a [Entity]-wide write lock until the [Entity.Tx] either commit
         * or rollback is issued.
         *
         * @param tupleIds The IDs of the [Tuple]s that should be deleted.
         *
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun deleteAll(tupleIds: Collection<Long>) {
            acquireWriteLock()
            try {
                /* Perform delete on each column. */
                tupleIds.forEach {tupleId ->
                    this.transactions.values.forEach { it.delete(tupleId) }
                }

                /* Update header. */
                val header = this@Entity.header
                header.size -= tupleIds.size
                header.modified = System.currentTimeMillis()
                store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            }
        }

        /**
         * Check if all the provided [Column]s exist on this [Entity] and that they have the type that was expected!
         *
         * @params The list of [Column]s that should be checked.
         */
        private fun checkColumnsExist(vararg columns: ColumnDef<*>) = columns.forEach {
            if (!transactions.contains(it)) {
                throw TransactionException.ColumnUnknownException(tid, it, this@Entity.name)
            }
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if ((tupleId < 0L) or (tupleId == HEADER_RECORD_ID)) {
                throw TransactionException.InvalidTupleId(tid, tupleId)
            }
        }

        /**
         * Checks if this [Column.Tx] is still open. Otherwise, an exception will be thrown.
         */
        @Synchronized
        private fun checkValidOrThrow() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }

        /**
         * Tries to acquire a write-lock. If method fails, an exception will be thrown
         */
        @Synchronized
        private fun acquireWriteLock() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
            if (this.status != TransactionStatus.DIRTY) {
                if (this@Entity.txLock.writeLock().tryLock()) {
                    this.status = TransactionStatus.DIRTY
                } else {
                    throw TransactionException.TransactionWriteLockException(this.tid)
                }
            }
        }
    }
}
