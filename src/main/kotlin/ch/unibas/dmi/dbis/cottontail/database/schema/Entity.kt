package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus

import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.TransactionException

import org.mapdb.*
import org.mapdb.volume.MappedFileVol
import java.io.IOException

import java.nio.file.Files
import java.nio.file.Path
import java.util.*

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import java.util.stream.Collectors

/** Type alias for a tuple. */
typealias Tuple = Map<ColumnDef,*>

/**
 * Represents an entity in the Cottontail DB data schema. An [Entity] has name that must remain unique within a [Schema].
 * The [Entity] contains one to many [Column]s holding the actual data.
 *
 * Calling the default constructor for [Entity] opens that [Entity]. It can only be opened once due to file locks and it
 * will remain open until the [Entity.close()] method is called.
 *
 * @see Schema
 * @see Column
 *
 * @see Entity.Tx
 */
internal class Entity(override val name: String, schema: Schema): DBO {
    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = schema.path.resolve("entity_$name")

    /** The fully qualified name of this [Entity] */
    override val fqn: String = "${schema.name}.$name"

    /** The parent [DBO], which is the [Schema] in case of an [Entity]. */
    override val parent: DBO? = schema

    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY)
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

    /**
     * List of all the [Column]s associated with this [Entity].
     */
    private val columns: List<Column<*>> = header.columns.map {
        Column<Any>(this.store.get(it, Serializer.STRING) ?:  throw DatabaseException.DataCorruptionException("Failed to open entity '$fqn': Could not read column at index $it!'"), this)
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
    val columnColumn: Int
        get() = this.header.columns.size

    /**
     * Closes the [Entity]. Closing an [Entity] is a delicate matter since ongoing [Entity.Tx] objects as well as all involved [Column]s are involved.
     * Therefore, access to the method is mediated by an global [Entity] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.closed = true
        this.columns.forEach { it.close() }
        this.store.close()
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
     *
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        private const val FILE_CATALOGUE = "index.mapdb"

        /** Filename for the [Entity] catalogue.  */
        private const val HEADER_RECORD_ID = 1L

        /** The identifier that is used to identify a Cottontail DB [Entity] file. */
        private const val HEADER_IDENTIFIER: String = "COTTONE"

        /** The version of the Cottontail DB [Entity]  file. */
        private const val HEADER_VERSION: Short = 1

        /** Initializes a new [Entity] at the given path. */
        internal fun initialize(name: String, schema: Schema, vararg columns: ColumnDef): Entity {
            /* Create empty folder for entity. */
            val data = schema.path.resolve("entity_$name")
            try {
                if (!Files.exists(data)) {
                    Files.createDirectories(data)
                } else {
                    throw DatabaseException("Failed to create entity '${schema.name}.$name'. Data directory '$data' seems to be occupied.")
                }
            } catch (e: IOException) {
                throw DatabaseException("Failed to create entity '${schema.name}.$name' due to an IO exception: {${e.message}")
            }

            /* Generate the store. */
            try {
                val store = StoreWAL.make(file = data.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY)
                store.preallocate() /* Pre-allocates the header. */

                /* Initialize the columns. */
                val columnIds = columns.map {
                    Column.initialize(data, it)
                    store.put(it.name, Serializer.STRING)
                }.toLongArray()
                store.update(HEADER_RECORD_ID, EntityHeader(columns = columnIds), EntityHeaderSerializer)
                store.commit()
                store.close()
                return Entity(name, schema)
            } catch (e: DBException) {
                val pathsToDelete = Files.walk(data).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
                throw DatabaseException("Failed to create entity '${schema.name}.$name' due to a storage exception: {${e.message}")
            }
        }
    }

    /**
     * A [Tx] that affects this [Entity].
     *
     * Opening such a [Tx] will spawn a associated [Column.Tx] for every [Column] associated with this [Entity].
     */
    inner class Tx(override val readonly: Boolean, override val tid: UUID = UUID.randomUUID()) : Transaction {
        /** List of [Column.Tx] associated with this [Entity.Tx]. */
        private val transactions: Map<ColumnDef, Column<*>.Tx> = mapOf(* this@Entity.columns.map { Pair(ColumnDef(it.name, it.type), it.Tx(readonly, tid)) }.toTypedArray())

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
            if (this.status == TransactionStatus.DIRTY) {
                this@Entity.store.rollback()
                this@Entity.txLock.writeLock().unlock()
            }
            this.status = TransactionStatus.CLOSED
            this@Entity.globalLock.readLock().unlock()
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
        fun read(tupleId: Long, vararg columns: ColumnDef): Tuple = this@Entity.txLock.read {
            checkValidOrThrow()
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
        fun <R> map(action: (Tuple) -> R, vararg columns: ColumnDef) = this@Entity.txLock.read {
            checkValidOrThrow()
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
        fun <R,T: Any> mapColumn(action: (T) -> R, column: ColumnDef): Collection<Pair<Long,R?>> = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(column)
            return this.transactions[column]!!.map { action(it as T)}
        }

        /**
         * Applies the provided function on each element found in this [Column].
         *
         * @param action The function to apply to each [Column] entry.
         * @param column The [ColumnSpec] that identifies the [Column] that should be mapped..
         */
        fun <T: Any> forEachColumn(action: (Long,T) -> Unit, column: ColumnDef) = this@Entity.txLock.read {
            checkValidOrThrow()
            checkColumnsExist(column)
            try {
                this.transactions[column]!!.forEach { l: Long, any: Any -> action(l, any as T)}
            } catch (e: TransactionException) {
                this.status = TransactionStatus.ERROR
                throw e
            }
        }

        /**
         * Attempts to insert the provided [Tuple] into the [Entity]. Columns specified in the [Tuple] that are not part
         * of the [Entity] will cause an error!
         *
         * @param tuple The [Tuple] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun insert(tuple: Tuple): Long? {
            checkColumnsExist(*tuple.keys.toTypedArray()) /* Perform sanity check on columns before locking. */
            acquireWriteLock()
            try {
                var lastRecId: Long? = null
                for ((column,tx) in this.transactions) {
                    val recId = (tx as Column<Any>.Tx).insert(tx.type.cast(tuple[column]))
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
        fun insertAll(tuples: Collection<Tuple>): Collection<Long?> {
            tuples.forEach { checkColumnsExist(*it.keys.toTypedArray()) }  /* Perform sanity check on columns before locking.. */
            acquireWriteLock()
            try {
                /* Perform delete on each column. */
                val tuplesIds = tuples.map {
                    var lastRecId: Long? = null
                    for ((column,tx) in this.transactions) {
                        val recId = (tx as Column<Any>.Tx).insert(tx.type.cast(it[column]))
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
         * Attempts to delete the provided [Tuple] from the [Entity]. This action will set this [Entity.Tx] to
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
         * Attempts to delete all the provided [Tuple] from the [Entity]. This action will set this [Entity.Tx] to
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
        private fun checkColumnsExist(vararg columns: ColumnDef) = columns.forEach {
            if (!transactions.contains(it) || transactions[it]!!.type != it.type) {
                throw TransactionException.ColumnUnknownException(tid, it, this@Entity.name)
            }
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if ((tupleId < 0L) or (tupleId == Entity.HEADER_RECORD_ID)) {
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

    /**
     * The header section of the [Entity] data structure.
     */
    private class EntityHeader(var size: Long = 0, var created: Long = System.currentTimeMillis(), var modified: Long  = System.currentTimeMillis(), var columns: LongArray = LongArray(0), var indexes: LongArray = LongArray(0))

    /**
     * The [Serializer] for the [EntityHeader].
     */
    private object EntityHeaderSerializer : Serializer<EntityHeader> {
        override fun serialize(out: DataOutput2, value: EntityHeader) {
            out.writeUTF(Entity.HEADER_IDENTIFIER)
            out.writeShort(Entity.HEADER_VERSION.toInt())
            out.packLong(value.size)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.writeShort(value.columns.size)
            value.columns.forEach { out.packLong(it) }
            out.writeShort(value.indexes.size)
            value.indexes.forEach { out.packLong(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityHeader {
            if (!this.validate(input)) {
                throw DatabaseException.InvalidFileException("Cottontail DB Entity")
            }
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

        /**
         * Validates the [EntityHeader]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val identifier = input.readUTF()
            val version = input.readShort()
            return (version == Entity.HEADER_VERSION) and (identifier == Entity.HEADER_IDENTIFIER)
        }
    }
}
