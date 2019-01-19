package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.model.DatabaseException

import org.mapdb.*
import org.mapdb.volume.MappedFileVol
import java.io.IOException
import java.nio.file.Files

import java.nio.file.Path
import java.util.*

import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.xml.crypto.Data
import kotlin.concurrent.read


/** Type alias for a tuple. */
typealias Tuple = Map<ColumnDef,*>

/**
 * Represents an entity in the Cottontail DB data schema. An [Entity] has name that must remain unique
 * in an instance of Cottontail and the [Entity] contains zero to many [Column]s holding the actual data.
 *
 * @see Column
 * @see Schema
 * @see Entity.Tx
 */
class Entity(val name: String, val path: Path) {
    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(name).resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity '$name' ($path): ${e.message}'.")
    }

    /** The header of this [Entity]. */
    private val header: EntityHeader = this.store.get(HEADER_RECORD_ID, EntityHeaderSerializer) ?: throw DatabaseException("Failed to open entity '$name' ($path): Could not read entity header!'")

    /** A internal lock that is used to synchronize [Transaction]s affecting this [Column]. */
    private val transactionLock = ReentrantReadWriteLock()

    /**
     * List of all the [Column]s associated with this [Entity].
     */
    private val columns: List<Column<*>> = header.columns.map {
        Column<Any>(this.store.get(it, Serializer.STRING) ?:  throw DatabaseException("Failed to open entity '$name' ($path): Could not read column at index $it!'"), path.resolve(name))
    }

    /**
     * Number of [Column]s held by this [Entity].
     */
    val columnColumn: Int
        get() = this.header.columns.size

    /**
     *
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        private const val FILE_CATALOGUE = "index.mapdb"

        /** Filename for the [Entity] catalogue.  */
        private const val HEADER_RECORD_ID = 1L

        /** Initializes a new [Entity] at the given path. */
        fun initialize(name: String, path: Path, vararg columns: ColumnDef): Entity {
            val data = path.resolve(name)
            if (!Files.exists(data)) {
                Files.createDirectories(data)
            } else {
                throw DatabaseException("Failed to create entity '$name'. Data directory '$data' seems to be occupied.")
            }

            /* Generate the store. */
            val store = StoreWAL.make(file = data.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY)
            store.preallocate() /* Pre-allocates the header. */

            /* Initialize the columns. */
            val columnIds = columns.map {
                Column.initialize(data, ColumnDef(it.first,it.second))
                store.put(it.first, Serializer.STRING)
            }.toLongArray()
            store.update(HEADER_RECORD_ID, EntityHeader(columns = columnIds), EntityHeaderSerializer)
            store.commit()
            store.close()

            return Entity(name, path)
        }
    }

    /**
     * A [Transaction] that affects this [Entity].
     *
     * Opening such a [Transaction] will spawn a associated [Column.Tx] for every [Column] associated with this [Entity].
     */
    inner class Tx(val readonly: Boolean, val tid: UUID = UUID.randomUUID()) : Transaction {
        /** List of [Column.Tx] associated with this [Entity.Tx]. */
        private val transactions: Map<ColumnDef,Column<*>.Tx> = mapOf(* this@Entity.columns.map { Pair(Pair(it.name, it.type), it.Tx(readonly, tid)) }.toTypedArray())

        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /**
         * Commits all changes made through this [Entity.Tx] since the last commit or rollback.
         */
        @Synchronized
        override fun commit() {
            if (this.status == TransactionStatus.DIRTY) {
                this.transactions.values.forEach { it.commit() }
                this@Entity.store.commit()
                this@Entity.transactionLock.writeLock().unlock()
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
                this@Entity.transactionLock.writeLock().unlock()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Entity.Tx] and thereby releases all the [Column.Tx] and locks. Closed [Entity.Tx] cannot be used anymore!
         */
        @Synchronized
        override fun close() {
            if (this.status == TransactionStatus.DIRTY) {
                this.rollback()
            }
            this.status = TransactionStatus.CLOSED
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
        fun read(tupleId: Long, vararg columns: ColumnDef): Tuple = this@Entity.transactionLock.read {
            checkOpenOrThrow()
            checkValidTupleId(tupleId)
            checkColumnsExist(*columns)

            /* Return value of all the desired columns. */
            return mapOf(* columns.map { Pair(it, transactions[it]!!.read(tupleId)) }.toTypedArray())
        }

        /**
         * Returns the number of entries in this [Entity].
         *
         * @return The number of entries in this [Entity].
         */
        fun count(): Long = this@Entity.transactionLock.read {
            checkOpenOrThrow()
            return this@Entity.header.size
        }

        /**
         *
         */
        fun <R> map(action: (Tuple) -> R, vararg columns: ColumnDef) = this@Entity.transactionLock.read {
            checkOpenOrThrow()
            checkColumnsExist(*columns)
        }

        /**
         * Applies the provided mapping function on each value found in this [Column], returning a collection of the desired output values.
         *
         * @param action The mapping that should be applied to each [Column] entry.
         * @param column The [ColumnSpec] that identifies the [Column] that should be mapped..
         *
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        fun <R,T: Any> mapColumn(action: (T) -> R, column: ColumnDef): Collection<Pair<Long,R?>> = this@Entity.transactionLock.read {
            checkOpenOrThrow()
            checkColumnsExist(column)
            return this.transactions[column]!!.map { action(it as T)}
        }

        /**
         * Applies the provided function on each element found in this [Column].
         *
         * @param action The function to apply to each [Column] entry.
         * @param column The [ColumnSpec] that identifies the [Column] that should be mapped..
         */
        fun <T: Any> forEachColumn(action: (Long,T) -> Unit, column: ColumnDef) = this@Entity.transactionLock.read {
            checkOpenOrThrow()
            checkColumnsExist(column)
            this.transactions[column]!!.forEach { l: Long, any: Any -> action(l, any as T)}
        }

        /**
         * Attempts to insert the provided [Tuple] into the [Entity]. Columns contained in the [Tuple] that are not part
         * of the [Entity] will be ignored!
         *
         * If insert fails for some reason, the [Entity] will make sure, that the [Column]s involved are rolled back.
         *
         * @param t The [Tuple] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws DatabaseException If something goes wrong during insert.
         */
        fun insert(t: Tuple): Long? {
            checkOpenOrThrow()
            acquireWriteLock()
            try {
                var lastRecId: Long? = null
                for ((column,tx) in this.transactions) {
                    val recId = (tx as Column<Any>.Tx).insert(tx.type.cast(t[column]))
                    if (lastRecId != recId && lastRecId != null) {
                        throw DatabaseException.DataCorruptionException("Entity ${this@Entity.name} is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastRecId = recId;
                }

                /* Update the header of this entity. */
                if (lastRecId != null) {
                    this@Entity.header.size += 1
                    this@Entity.header.modified = System.currentTimeMillis()
                    this@Entity.store.update(HEADER_RECORD_ID, this@Entity.header, EntityHeaderSerializer)
                }

                return lastRecId
            } catch (e: DatabaseException) {
                this@Entity.store.rollback()
                throw e
            }
        }

        /**
         * Attempts to delete the provided [Tuple] from the [Entity].
         *
         * If delete fails for some reason, the [Entity] will make sure, that the [Column]s involved are rolled back.
         *
         * @param tupleId The ID of the  [Tuple] that should be deleted.
         * @throws DatabaseException If something goes wrong during insert.
         */
        fun delete(tupleId: Long) {
            checkOpenOrThrow()
            acquireWriteLock()
            try {
                /* Perform delete on each column. */
                this.transactions.values.forEach { it.delete(tupleId) }

                /* Update header. */
                this@Entity.header.size -= 1
                this@Entity.header.modified = System.currentTimeMillis()
                this@Entity.store.update(HEADER_RECORD_ID, this@Entity.header, EntityHeaderSerializer)

            } catch (e: DatabaseException) {
                this@Entity.store.rollback()
                throw e
            }
        }

        /**
         * Check if all the provided column names exist on this [Entity] and that they have the type that was expected!
         */
        private fun checkColumnsExist(vararg columns: ColumnDef) = columns.forEach {
            if (!transactions.contains(it)) {
                throw DatabaseException.ColumnNotExistException(it.first, this@Entity.name)
            }
            if (transactions[it]!!.type != it.second) {
                throw DatabaseException.ColumnTypeUnexpectedException(it.first, this@Entity.name, it.second, transactions[it]!!.type)
            }
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if ((tupleId < 0L) or (tupleId == Entity.HEADER_RECORD_ID)) {
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
                if (this@Entity.transactionLock.writeLock().tryLock()) {
                    this.status = TransactionStatus.DIRTY
                } else {
                    throw DatabaseException.TransactionLockException(this.tid)
                }
            }
        }
    }
}

/**
 * The header section of the [Entity] data structure.
 */
class EntityHeader(var size: Long = 0, var created: Long = System.currentTimeMillis(), var modified: Long  = System.currentTimeMillis(), var columns: LongArray = LongArray(0), var indexes: LongArray = LongArray(0))

/**
 * The [Serializer] for the [EntityHeader].
 */
object EntityHeaderSerializer : Serializer<EntityHeader> {
    override fun serialize(out: DataOutput2, value: EntityHeader) {
        out.packLong(value.size)
        out.writeLong(value.created)
        out.writeLong(value.modified)
        out.writeShort(value.columns.size)
        value.columns.forEach { out.packLong(it) }
        out.writeShort(value.indexes.size)
        value.indexes.forEach { out.packLong(it) }
    }

    override fun deserialize(input: DataInput2, available: Int): EntityHeader {
        val size = input.unpackLong()
        val created = input.readLong()
        val modified = input.readLong()
        val columns = LongArray(input.readShort().toInt())
        for (i in 0 until columns.size) {
            columns[i] = input.unpackLong()
        }
        val indexes = LongArray(input.readShort().toInt())
        for (i in 0 until indexes.size) {
            indexes[i] = input.unpackLong()
        }
        return EntityHeader(size, created, modified, columns, indexes)
    }
}
