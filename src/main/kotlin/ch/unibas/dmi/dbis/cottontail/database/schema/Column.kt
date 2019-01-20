package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.model.DatabaseException

import org.mapdb.*
import org.mapdb.volume.MappedFileVol

import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


/** Typealias for a definition of a column. */
typealias ColumnDef = Pair<String,ColumnType<*>>

/**
 * Represents a single column in the Cottontail DB schema. A [Column] record is identified by a tuple
 * ID (long) and can hold an arbitrary value.
 *
 * @param <T> Type of the value held by this [Column].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Column<T: Any>(val name: String, val path: Path): DBO {
    /** Internal reference to the [Store] underpinning this [Column]. */
    private var store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve("col_$name.db").toString(), volumeFactory = MappedFileVol.FACTORY)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the [Header] of this [Column]. */
    private val header = try {
        store.get(HEADER_RECORD_ID, ColumnHeaderSerializer) ?: throw DatabaseException("Failed to open column $path: Could not read column header!'")
    } catch (e: DatabaseException) {
        throw DatabaseException("Failed to open column at $path: ${e.message}'")
    }

    /**
     * Getter for [Column.definition].
     *
     * @return The [ColumnType] of this [Column].
     */
    val type: ColumnType<T>
        get() = this.header.type as ColumnType<T>

    /**
     * Status indicating whether this [Column] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /** A internal lock that is used to synchronize [Column.Tx]s affecting this [Column]. */
    private val txLock = ReentrantReadWriteLock()

    /** A internal lock that is used to synchronize closing of an [Column] with running [Column.Tx]. */
    private val globalLock = ReentrantReadWriteLock()

    /**
     * Closes the [Column]. Closing an [Column] is a delicate matter since ongoing [Column.Tx]  are involved.
     * Therefore, access to the method is mediated by an global [Column] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.closed = true
        this.store.close()
    }

    /**
     * Companion object with some important constants.
     */
    companion object {
        /** Record ID of the [ColumnHeader]. */
        private const val HEADER_RECORD_ID: Long = 1L

        /** The identifier that is used to identify a Cottontail DB [Column] file. */
        private const val HEADER_IDENTIFIER: String = "COTTONC"

        /** The version of the Cottontail DB [Column]  file. */
        private const val HEADER_VERSION: Short = 1

        /**
         * Initializes a new, empty [Column]
         *
         * @param parent The folder that contains the data file.
         * @param definition The [ColumnDef] that specified the [Column]
         */
        fun initialize(parent: Path, definition: ColumnDef) {
            if (!Files.exists(parent)) {
                Files.createDirectories(parent)
            }
            val store = StoreWAL.make(file = parent.resolve("col_${definition.first}.db").toString(), volumeFactory = MappedFileVol.FACTORY)
            store.put(ColumnHeader(type = definition.second, size = 0), ColumnHeaderSerializer)
            store.commit()
            store.close()
        }
    }

    /**
     * A [Transaction] that affects this [Column].
     */
    inner class Tx(val readonly: Boolean, val tid: UUID = UUID.randomUUID()): Transaction {
        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** Tries to acquire a global read-lock on the [Column]. */
        init {
            this@Column.globalLock.read {
                if (this@Column.closed) {
                    throw DatabaseException.TransactionDBOClosedException(tid)
                }
                this@Column.globalLock.readLock().tryLock()
            }
        }

        /**
         * Commits all changes made through this [Transaction] since the last commit or rollback.
         */
        @Synchronized
        override fun commit() {
            if (this.status == TransactionStatus.DIRTY) {
                this@Column.store.commit()
                this.status = TransactionStatus.CLEAN
                this@Column.txLock.writeLock().unlock()
            }
        }

        /**
         * Rolls all changes made through this [Transaction] back to the last commit.
         */
        @Synchronized
        override fun rollback() {
            if (this.status == TransactionStatus.DIRTY) {
                this@Column.store.rollback()
                this.status = TransactionStatus.CLEAN
                this@Column.txLock.writeLock().unlock()
            }
        }

        /**
         * Closes this [Transaction] and relinquishes the associated [ReentrantReadWriteLock].
         */
        @Synchronized
        override fun close() {
            if (this.status == TransactionStatus.DIRTY) {
                this.rollback()
            }
            this.status = TransactionStatus.CLOSED
            this@Column.globalLock.readLock().unlock()
        }

        /**
         * Gets and returns an entry from this [Column]. Action acquires a global read dataLock for the [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        fun read(tupleId: Long) : T? = this@Column.txLock.read {
            checkOpenOrThrow()
            checkValidTupleId(tupleId)
            return this@Column.store.get(tupleId, this@Column.type.serializer)
        }

        /**
         * Returns the number of entries in this [Column]. Action acquires a global read dataLock for the [Column].
         *
         * @return The number of entries in this [Column].
         */
        fun count(): Long = this@Column.txLock.read {
            return this@Column.header.size
        }

        /**
         * Applies the provided mapping function on each value found in this [Column], returning a
         * collection of the desired output values.
         *
         * @param action The action that should be applied.
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        fun <R> map(action: (T?) -> R?): Collection<Pair<Long,R?>> = this@Column.txLock.read {
            checkOpenOrThrow()
            val list = mutableListOf<Pair<Long,R?>>()
            this@Column.store.getAllRecids().forEach {
                list.add(Pair(it,action(this.read(it))))
            }
            return list
        }

        /**
         * Applies the provided function on each element found in this [Column].
         */
        fun forEach(action: (Long,T) -> Unit) = this@Column.txLock.read {
            checkOpenOrThrow()
            this@Column.store.getAllRecids().forEach {
                if (it != HEADER_RECORD_ID) {
                    action(it,this.read(it)!!)
                }
            }
        }

        /**
         * Inserts a new record in this [Column].
         *
         * @param record The record that should be inserted. Can be null!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        fun insert(record: T?): Long {
            acquireWriteLock()
            val tupleId = if (record == null) {
                this@Column.store.preallocate()
            } else {
                this@Column.store.put(record, this@Column.type.serializer)
            }

            /* Update header. */
            this@Column.header.size += 1
            this@Column.header.modified = System.currentTimeMillis()
            this@Column.store.update(HEADER_RECORD_ID, this@Column.header, ColumnHeaderSerializer)
            return tupleId
        }

        /**
         * Inserts a list of new records in this [Column].
         *
         * @param records The records that should be inserted. Can contain null values!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        fun insertAll(records: Collection<T?>): Collection<Long> {
            acquireWriteLock()
            val tupleIds = records.map {
                if (it == null) {
                this@Column.store.preallocate()
            } else {
                this@Column.store.put(it, this@Column.type.serializer)
            } }

            /* Update header. */
            this@Column.header.size += records.size
            this@Column.header.modified = System.currentTimeMillis()
            this@Column.store.update(HEADER_RECORD_ID, this@Column.header, ColumnHeaderSerializer)
            return tupleIds
        }

        /**
         * Deletes a record from this [Column]. Action acquires a global write dataLock for the [Column].
         *
         * @param tupleId The ID of the record that should be deleted
         * @return The tupleId of the inserted record.
         */
        fun delete(tupleId: Long) {
            acquireWriteLock()
            checkValidTupleId(tupleId)
            this@Column.store.delete(tupleId, this@Column.type.serializer)

            /* Update header. */
            this@Column.header.size -= 1
            this@Column.header.modified = System.currentTimeMillis()
            this@Column.store.update(HEADER_RECORD_ID, this@Column.header, ColumnHeaderSerializer)
        }

        /**
         *
         */
        fun deleteAll(tupleIds: Collection<Long>) {
            acquireWriteLock()
            tupleIds.forEach{
                checkValidTupleId(it)
                this@Column.store.delete(it, this@Column.type.serializer)
            }

            /* Update header. */
            this@Column.header.size -= tupleIds.size
            this@Column.header.modified = System.currentTimeMillis()
            this@Column.store.update(HEADER_RECORD_ID, this@Column.header, ColumnHeaderSerializer)
        }

        /**
         * Returns the [ColumnType] of the [Column] associated with this [Column.Tx].
         *
         * @return [ColumnType]
         */
        val type: ColumnType<T>
            get() = this@Column.type

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if ((tupleId < 0L) or (tupleId == HEADER_RECORD_ID)) {
                throw DatabaseException.InvalidTupleId(tupleId)
            }
        }

        /**
         * Checks if this [Column.Tx] is still open. Otherwise, an exception will be thrown.
         */
        @Synchronized
        private fun checkOpenOrThrow() {
            if (this.status == TransactionStatus.CLOSED) throw DatabaseException.TransactionClosedException(tid)
        }

        /**
         * Tries to acquire a write-lock. If method fails, an exception will be thrown
         */
        @Synchronized
        private fun acquireWriteLock() {
            if (this.readonly) throw DatabaseException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw DatabaseException.TransactionClosedException(tid)
            if (this.status != TransactionStatus.DIRTY) {
                if (this@Column.txLock.writeLock().tryLock()) {
                    this.status = TransactionStatus.DIRTY
                } else {
                    throw DatabaseException.TransactionLockException(this.tid)
                }
            }
        }
    }

    /**
     * The header data structure of any [Column]
     */
    class ColumnHeader(val type: ColumnType<*>, var size: Long, var created: Long = System.currentTimeMillis(), var modified: Long = System.currentTimeMillis())

    /**
     * A [Serializer] for [ColumnHeader].
     */
    object ColumnHeaderSerializer: Serializer<ColumnHeader> {
        override fun serialize(out: DataOutput2, value: ColumnHeader) {
            out.writeUTF(Column.HEADER_IDENTIFIER)
            out.writeShort(Column.HEADER_VERSION.toInt())
            out.writeUTF(value.type.name)
            out.packLong(value.size)
            out.writeLong(value.created)
            out.writeLong(value.modified)
        }

        override fun deserialize(input: DataInput2, available: Int): ColumnHeader {
            if (!this.validate(input)) {
                throw DatabaseException.InvalidFileException("Cottontail DB Column")
            }
            return ColumnHeader(ColumnType.typeForName(input.readUTF()), input.unpackLong(), input.readLong(), input.readLong())
        }

        /**
         * Validates the [ColumnHeader]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val identifier = input.readUTF()
            val version = input.readShort()
            return (version == Column.HEADER_VERSION) and (identifier == Column.HEADER_IDENTIFIER)
        }
    }
}

