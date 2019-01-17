package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.database.definition.ColumnDefinition
import ch.unibas.dmi.dbis.cottontail.database.definition.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.definition.EntityDefinition
import ch.unibas.dmi.dbis.cottontail.database.general.AccessorMode
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.model.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.LockedException
import ch.unibas.dmi.dbis.cottontail.serializer.schema.Serializers
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.mapdb.*

import java.io.IOException

import java.nio.file.Files
import java.nio.file.Path
import java.util.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Collectors

/**
 * Represents an entity in the Cottontail DB data schema. A [Entity] has name that must remain unique in an instance of Cottontail and the [Entity]
 * contains zero to many [Column]s holding the actual data.
 *
 * This class mediates atomic access to the [Entity], its [Column]s and the underlying data store by the means of [Transaction]s. The class
 * is thread safe in the sense, that access to the data store is properly handled as long as different threads use the same [Entity] instance for the same
 * entity. So for a given entity, the corresponding [Entity] instance is effectively a singleton. It is up to the [Schema] to assure that.
 *
 * @see Column
 *
 * @see Schema
 *
 * @see Entity.Tx
 */
class Entity
/**
 * Constructor for [Entity].
 *
 * @param def [EntityDefinition] from which to construct a new [Entity].
 */
@Throws(IOException::class)
internal constructor(private val definition: EntityDefinition, private val lock_timeout_ms: Int) {

    /** A map containing all the named columns in this [Entity].  */
    private val columns = HashMap<String, Column<*>>()

    /** An internal flag indicating whether this [Entity] is locked (i.e. no new transactions can be created).  */
    private val catalogueLock = ReentrantReadWriteLock()

    /** An internal flag indicating whether this [Entity]'s tuple ID counter is locked.  */
    private val tupleIdLock = ReentrantReadWriteLock()

    /**
     * Getter for [Entity.definition].
     *
     * @return Name of [Entity]
     */
    val name: String
        get() = this.definition.name

    /**
     * Getter for [Entity.definition].
     *
     * @return Path to the [Entity]
     */
    internal val path: Path
        get() = this.definition.path

    init {
        /* Create folder (if it doesn't exist). */
        if (!Files.exists(this.definition.path)) {
            Files.createDirectories(this.definition.path)
        }

        /* Open / create the catalogue and its entries. */
        val catalogue = DBMaker.fileDB(this.definition.path.resolve(FILE_CATALOGUE).toFile()).fileMmapEnableIfSupported().transactionEnable().make()
        val columns = catalogue.hashMap(PROPERTY_COLUMNS)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializers.COLUMN_DEF_SERIALIZER)
                .createOrOpen()


        for ((key, value) in columns) {
            this.columns[key] = Column(value)
        }

        /* Open / create the catalogue and its entries. */
        val rowid = DBMaker.fileDB(this.definition.path.resolve(FILE_ROWID).toFile()).fileMmapEnableIfSupported().allocateStartSize(8).allocateIncrement(8).fileMmapEnableIfSupported().make()
        rowid.atomicLong(PROPERTY_TUPLEID, 0).createOrOpen()

        /* Update basic properties. */
        val now = System.currentTimeMillis()
        catalogue.atomicLong(PROPERTY_LAST_MODIFIED, now).createOrOpen()
        catalogue.atomicLong(PROPERTY_CREATED, now).createOrOpen()
        catalogue.atomicInteger(PROPERTY_VERSION, 0).createOrOpen()

        /* Commit changes to entity. */
        catalogue.commit()
        rowid.commit()

        /* Close files. */
        catalogue.close()
        rowid.close()
    }

    /**
     *
     * @return
     */
    fun <T : Any> columnForName(name: String, type: ColumnType<T>): Column<T>? {
        val column = this.columns[name]
        return if (column != null && column.type == type) {
            column as Column<T>?
        } else {
            null
        }
    }

    /**
     * Adds a new [Column] to this [Entity]. Since [Entity]'s catalogue needs to be updated in order to perform this operation
     * calling this method will lock the entire [Entity].
     *
     * @throws DatabaseException If an error occurred with the underlying database while adding a column.
     * @throws LockedException If method failed to lock the catalogue.
     */
    @Synchronized
    @Throws(DatabaseException::class)
    fun <T : Any> createColumn(name: String, type: ColumnType<T>) {
        try {
            if (this.catalogueLock.writeLock().tryLock(this.lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) {
                try {
                    /* Check if column exists. */
                    if (columns.containsKey(name)) {
                        throw DatabaseException("Failed to create column. The column '%s' already exists on entity '%s'.", name, this.definition.name)
                    }

                    /* Make changes to catalogue. */
                    DBMaker.fileDB(this.definition.path.resolve(FILE_CATALOGUE).toFile()).fileMmapEnableIfSupported().transactionEnable().make().use { catalogue ->
                        /* Create and store new ColumnDefinition. */
                        val def = ColumnDefinition(name, this.definition.path.resolve(String.format("c_%s.mapdb", name)), type)
                        val col = Column(def)

                        /* Update catalogue. */
                        catalogue.hashMap(PROPERTY_COLUMNS).keySerializer(Serializer.STRING).valueSerializer(Serializers.COLUMN_DEF_SERIALIZER).open()[name] = def
                        catalogue.atomicLong(PROPERTY_LAST_MODIFIED).open().set(System.currentTimeMillis())
                        catalogue.atomicInteger(PROPERTY_VERSION).open().incrementAndGet()
                        catalogue.commit()

                        /* Put new column and commit. */
                        this.columns.put(name, col)
                    }
                } finally {
                    this.catalogueLock.writeLock().unlock() /* Relinquish lock on catalogue. */
                }
            } else {
                throw LockedException("Failed to acquire write-lock on catalogue of entity '%s'. Timeout of %dms has elapsed.", this.definition.name, this.lock_timeout_ms)
            }
        } catch (e: InterruptedException) {
            throw LockedException("Failed to acquire write-lock on catalogue of entity '%s'. Thread was interrupted while waiting for lock to become free.", this.definition.name)
        }
    }

    /**
     * Removes a [Column] from this [Entity]. Since [Entity]'s catalogue needs to be updated in order to perform this operation
     * calling this method will lock the entire [Entity].
     *
     * @throws DatabaseException If an error occurred with the underlying database while removing a column.
     * @throws LockedException If method failed to lock the catalogue.
     */
    @Synchronized
    @Throws(DatabaseException::class)
    fun dropColumn(name: String) {
        try {
            if (this.catalogueLock.writeLock().tryLock(this.lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) { /* Acquire write-lock on catalogue. */
                try {
                    /* Unmount column from this catalogue. */
                    val column = this.columns.remove(name)
                            ?: throw DatabaseException("Failed to drop column. The column '%s' does not exists on entity '%s'.", name, this.definition.name)

                    /* Make changes to catalogue. */
                    try {
                        DBMaker.fileDB(this.definition.path.resolve(FILE_CATALOGUE).toFile()).fileMmapEnableIfSupported().transactionEnable().make().use { catalogue ->
                            /* Remove entry from catalogue. */
                            catalogue.hashMap(PROPERTY_COLUMNS).keySerializer(Serializer.STRING).valueSerializer(Serializers.COLUMN_DEF_SERIALIZER).open().remove(name) /* Update catalogue. */
                            catalogue.atomicLong(PROPERTY_LAST_MODIFIED).open().set(System.currentTimeMillis())
                            catalogue.atomicInteger(PROPERTY_VERSION).open().incrementAndGet()
                            catalogue.commit()
                        }
                    } catch (e: Exception) {
                        this.columns[name] = column
                        LOGGER.log(Level.ERROR, "Failed to drop column '{}'. Could not update catalogue due to exception: {}", name, e)
                        throw DatabaseException("Failed to drop column '%s'. Could not update catalogue due to an exception.", name)
                    }

                    /* Delete associated files. */
                    try {
                        val files = Files.walk(column.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                        for (p in files) Files.deleteIfExists(p)
                    } catch (e: IOException) {
                        LOGGER.log(Level.WARN, "Could not delete files associated with column '{}' due to an IOException: {}", name, e)
                    }

                } finally {
                    this.catalogueLock.writeLock().unlock() /* Relinquish write-lock on catalogue. */
                }
            } else {
                throw LockedException("Failed to drop column '%s'. Could not acquire write-lock on catalogue of entity '%s'. Timeout of %dms has elapsed.", name, this.definition.name, this.lock_timeout_ms)
            }
        } catch (e: InterruptedException) {
            throw LockedException("Failed to drop column '%s'. Could not acquire write-lock on catalogue of entity '%s'. Thread was interrupted while waiting for lock to become free.", name, this.definition.name)
        }

    }

    /**
     * Creates and returns a new [Entity.Tx]
     *
     * @param columns The name of the [Column]s for which the [Tx] should be created.
     * @param mode The [Tx]'s [AccessorMode]
     * @return [Entity.Tx]
     *
     * @throws LockedException If [Entity] has been locked.
     * @throws DatabaseException If an error occurred while creating the [Tx].
     */
    @Throws(LockedException::class, DatabaseException::class)
    fun newTransaction(columns: Collection<String>, mode: AccessorMode): Tx {
        return Tx(columns, mode)
    }

    /**
     * Represents a single read / write transaction on the enclosing [Entity]. This [Tx] may involve multiple [Column.Tx] objects.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class Tx constructor(columnNames: Collection<String>, override val mode: AccessorMode) : Transaction {

        /** The [Column.Tx]s that make up this [Tx].  */
        private val columns = HashMap<String, Column<*>.Tx>()

        /** The ID of this [Column.Tx].  */
        private val txid: UUID

        /** Reference to [Entity]'s catalogue (read-only).  */
        private val catalogue: DB

        /** Reference to the file that holds the [Entity]'s tuple ID.  */
        private val tupleid: DB

        /** Whether this [Column.Tx] as conducted and insert operation. */
        private var insert: Boolean = false

        /** The current [TransactionStatus] of this [Column.Tx].  */
        @Volatile
        override var status = TransactionStatus.CLEAN

        init {
            this.txid = UUID.randomUUID()
            try {
                if (this@Entity.catalogueLock.readLock().tryLock(this@Entity.lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) {
                    /* Get read-only reference to catalogue. */
                    this.catalogue = DBMaker.fileDB(this@Entity.definition.path.resolve(FILE_CATALOGUE).toFile()).readOnly().fileMmapEnableIfSupported().transactionEnable().make()

                    /* Check columns (they must exist). */
                    for (name in columnNames) {
                        val column = this@Entity.columns[name]
                        if (column != null) {
                            this.columns[name] = column.getTransaction(txid, mode)
                        } else {
                            this.close()
                            throw DatabaseException("Failed to create transaction. The provided column '%s' does not exist on entity '%s.", name, this@Entity.definition.name)
                        }
                    }

                    /* Open file that holds current tuple ID for entity. */
                    when (mode) {
                        AccessorMode.READONLY -> this.tupleid = DBMaker.fileDB(this@Entity.definition.path.resolve(FILE_ROWID).toFile()).fileMmapEnableIfSupported().readOnly().make()
                        AccessorMode.READWRITE -> this.tupleid = DBMaker.fileDB(this@Entity.definition.path.resolve(FILE_ROWID).toFile()).fileMmapEnableIfSupported().make()
                        AccessorMode.READWRITE_TX -> this.tupleid = DBMaker.fileDB(this@Entity.definition.path.resolve(FILE_ROWID).toFile()).fileMmapEnableIfSupported().transactionEnable().make()
                    }
                } else {
                    throw LockedException("Failed to lock catalogue for entity '%s'. Timeout of %dms has elapsed.", this@Entity.definition.name, this@Entity.lock_timeout_ms)
                }
            } catch (e: InterruptedException) {
                throw LockedException("Failed to lock catalogue for entity '%s'. Thread was interrupted while waiting for lock to become free.", this@Entity.definition.name)
            }

        }

        /**
         * Returns the current tuple ID value.
         *
         * @return Current tuple ID value.
         * @throws LockedException If no read-lock could be obtained for tuple ID field.
         */
        @Throws(DatabaseException::class)
        fun currentTid(): Long {
            return try {
                if (this.insert || this@Entity.tupleIdLock.readLock().tryLock(lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) {
                    this.tupleid.atomicLong(PROPERTY_TUPLEID).open().get()
                } else {
                    throw LockedException("Could not obtain read-lock on row ID for entity '%s'. Timeout of %dms exceeded.", this@Entity.definition.name, this@Entity.lock_timeout_ms)
                }
            } catch (e: InterruptedException) {
                throw LockedException("Could not obtain read-lock on row ID for entity '%s'. Thread was interrupted while waiting for lock to become free.")
            }
        }

        /**
         *
         * @param tid
         * @return
         */
        operator fun get(tid: Long): Map<String, Any?> {
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be used to read from the entity '%s'.", this.txid, this@Entity.definition.name))
            val result = HashMap<String, Any?>(this.columns.size)
            for ((key, value) in this.columns) {
                result[key] = value.read(tid)
            }
            return result
        }

        /**
         *
         * @param values
         * @throws DatabaseException
         */
        @Synchronized
        @Throws(DatabaseException::class)
        fun insert(values: Map<String, *>) {
            if (this.mode === AccessorMode.READONLY) throw IllegalStateException(String.format("Transactional '%s' is read-only; it cannot be used to make any changes to the entity '%s'.", this.txid, this@Entity.definition.name))
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be used to make any changes to the entity '%s'.", this.txid, this@Entity.definition.name))
            var error: DatabaseException? = null
            try {
                if (this@Entity.tupleIdLock.writeLock().tryLock(lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) {
                    /* Update status. */
                    this.status = TransactionStatus.DIRTY
                    this.insert = true

                    /* Perform insert. */
                    val currentId = this.tupleid.atomicLong(PROPERTY_TUPLEID).open().andIncrement
                    for ((key, value) in values) {
                        val ctx = this.columns[key]
                        if (ctx != null) {
                            val type = ctx.type().cast(value!!)
                            if (type != null) {
                                if (!(ctx as Column<Any>.Tx).writeIfAbsent(currentId, type)) {
                                    error = DatabaseException("Failed to insert. An internal error occurred when inserting value for column '%s'.", key)
                                    break
                                }
                            } else {
                                error = DatabaseException("Failed to insert. The type of the provided column '%s' (%s) is not compatible with the provided type %s.", key, ctx.type(), value!!.javaClass.simpleName)
                                break
                            }
                        } else {
                            error = DatabaseException("Failed to insert. The provided column '%s' has not been defined in transaction.", key)
                            break
                        }
                    }

                    /* On error: rollback changes. */
                    if (error != null) {
                        this.rollback()
                        throw error
                    }
                } else {
                    throw LockedException("Could not obtain lock on row ID for entity '%s'. Timeout of %dms exceeded.", this@Entity.definition.name, this@Entity.lock_timeout_ms)
                }
            } catch (e: InterruptedException) {
                throw LockedException("Could not obtain lock on row ID for entity '%s'. Thread was interrupted while waiting for lock to become free.")
            }
        }

        /**
         * Commits all changes that were made since the last commit. Causes the [Transaction] to complete and close.
         *
         * Only works, if [Column.Tx] has been created in mode [AccessorMode.READWRITE_TX].
         * Otherwise, calling this method has no effect.
         */
        @Synchronized
        override fun commit() {
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be committed.", this.txid))
            if (this.status === TransactionStatus.DIRTY) {
                this.tupleid.commit()
                this.columns.values.forEach { it -> it.commit() }
                if (this.insert) {
                    this@Entity.tupleIdLock.writeLock().unlock()
                    this.insert = false;
                }
                this.status == TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls back all changes that were made since the last commit. Causes the [Transaction] to complete and close.
         *
         * Rollback only works, if [Column.Tx] has been created in mode [AccessorMode.READWRITE_TX].
         * Otherwise, calling this method has no effect.
         */
        @Synchronized
        override fun rollback() {
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be committed.", this.txid))
            if (this.status === TransactionStatus.DIRTY) {
                this.tupleid.rollback()
                this.columns.values.forEach { it -> it.rollback() }
                if (this.insert) {
                    this@Entity.tupleIdLock.writeLock().unlock()
                    this.insert = false;
                }
                this.status == TransactionStatus.CLEAN
            }
        }

        /**
         * Closes the underlying [DB] and ends this [Entity.Tx].
         */
        @Synchronized
        override fun close() {
            /* Return if Transactional is already closed. */
            if (this.status === TransactionStatus.CLOSED) return

            /* Close catalogue and tuple. */
            this.catalogue.close()
            this.tupleid.close()

            /* Close sub-transactions. */
            this.columns.values.forEach {it -> it.close()}

            /* Relinquish the catalogue level read-lock that have been obtained by this Transactional. */
            this.status = TransactionStatus.CLOSED
            this@Entity.catalogueLock.readLock().unlock()
        }
    }

    companion object {
        /** Filename for the [Entity] catalogue.  */
        private const val FILE_CATALOGUE = "index.mapdb"

        /** Filename for the [Entity] tuple ID file.  */
        private const val FILE_ROWID = "tid.mapdb"

        /** Name of the tuple ID property (i.e. current value of the tuple ID).  */
        private const val PROPERTY_TUPLEID = "tid"

        /** Name of the [Entity]'s columns property.  */
        private const val PROPERTY_COLUMNS = "columns"

        /** Name of the [Entity]'s columns modified property.  */
        private const val PROPERTY_LAST_MODIFIED = "modified"

        /** Name of the [Entity]'s created property.  */
        private const val PROPERTY_CREATED = "created"

        /** Name of the [Entity]'s version property.  */
        private const val PROPERTY_VERSION = "version"

        /** The [Logger] instance used to log errors and information.  */
        private val LOGGER = LogManager.getLogger()
    }
}
