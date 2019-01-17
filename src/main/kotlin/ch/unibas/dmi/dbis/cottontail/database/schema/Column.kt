package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.database.definition.ColumnDefinition
import ch.unibas.dmi.dbis.cottontail.database.definition.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.general.AccessorMode
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus

import org.mapdb.*

import java.nio.file.Path
import java.util.Optional
import java.util.UUID

/**
 * A single column in the Cottontail DB schema. A [Column] entry is identified by a tuple ID (long) and holds an arbitrary value.
 *
 * @param <T> Type of the value held by this [Column].
 *
 * @author Ralph Gasser
 * @version 1.0
</T> */
class Column<T : Any>(private val definition: ColumnDefinition<T>) {
    /**
     * Getter for [Column.definition].
     *
     * @return Name of this [Column].
     */
    val name: String
        get() = this.definition.name

    /**
     * Getter for [Column.definition].
     *
     * @return Name of this [Column].
     */
    val path: Path
        get() = this.definition.path

    /**
     * Getter for [Column.definition].
     *
     * @return The [ColumnType] of this [Column].
     */
    val type: ColumnType<T>
        get() = this.definition.type

    /**
     * Generates and returns a new [Tx] for this [Column].
     *
     * @param mode The [AccessorMode] of the new [Tx].
     *
     * @return New [Tx]
     */
    fun getTransaction(mode: AccessorMode): Tx {
        return getTransaction(UUID.randomUUID(), mode)
    }

    /**
     * Generates and returns a new [Tx] for this [Column].
     *
     * @param txid The transaction ID of the new [Tx].
     * @param mode The [AccessorMode] of the new [Tx].
     *
     * @return New [Tx]
     */
    fun getTransaction(txid: UUID, mode: AccessorMode): Tx {
        return Tx(txid, mode)
    }

    /**
     * Represents a single read / write transaction on the enclosing [Column].
     */
    inner class Tx constructor(private val txid: UUID, override val mode: AccessorMode) : Transaction {

        /** Reference to the [DB] object associated with this [Tx].  */
        private val database: DB

        /** Reference to the [HTreeMap] object associated with this [Tx].  */
        private val map: BTreeMap<Long,T>

        /** The current [TransactionStatus] of this [Tx].  */
        @Volatile
        override var status = TransactionStatus.CLEAN

        /**
         * Initializes [Tx].
         */
        init {
            /* Open / create the database file for column. */
            var maker: DBMaker.Maker = DBMaker.fileDB(this@Column.definition.path.toFile())
                    .concurrencyScale(Runtime.getRuntime().availableProcessors())
                    .fileMmapEnableIfSupported()
            when (mode) {
                AccessorMode.READONLY -> maker = maker.readOnly()
                AccessorMode.READWRITE_TX -> maker = maker.transactionEnable()
                else -> {}
            }
            this.database = maker.make()

            /* Generate the HTreeMap for this Transactional. */
            this.map = this.database
                    .treeMap(this@Column.name, Serializer.RECID, this@Column.type.serializer)
                    .counterEnable()
                    .createOrOpen()
        }

        /**
         * Returns the type of the value returned by this [Column.Tx].
         */
        fun type() : ColumnType<T> {
           return this@Column.type
        }

        /**
         * Accesses and returns the entry for the specified tuple ID.
         *
         * @param tid The tuple ID of the entry to return.
         * @return Value of the entry specified by txid or null, if it is not defined.
         */
        fun read(tid: Long): T? {
            return this.map[tid]
        }

        /**
         * Checks if there is an entry for the specified tuple ID.
         *
         * @param tid The tuple ID of the entry to check.
         * @return True if entry exists, false otherwise.
         */
        fun exists(tid: Long): Boolean {
            return this.map.containsKey(tid)
        }

        /**
         * Returns the number of entries in this [Column].
         *
         * @return Number of entries in this [Column].
         */
        fun count(): Int {
            return this.map.size
        }

        /**
         * Returns an [Iterable] that can be used to scan all the entries..
         *
         * @return Iterable<T> of all the [Column]'s entries.
        </T> */
        fun scan(): Sequence<Map.Entry<Long, T>> = this.map.asSequence()

        /**
         * Returns an [Iterable] that can be used to scan all the values.
         *
         * @return Iterable<T> of all the [Column]'s values.
        </T> */
        fun scanValues(): Sequence<T?> = this.map.values.asSequence()

        /**
         * Inserts the provided value for the provided tuple ID if, and only if, no value exists for the provided Tuple ID.
         *
         * @param tid The Tuple ID for which to insert a value.
         * @param value The value to insert.
         * @return True if value was inserted, false otherwise.
         * @throws IllegalStateException If [Tx] is either read-only or has been closed.
         */
        @Synchronized
        fun writeIfAbsent(tid: Long, value: T): Boolean {
            if (this.mode === AccessorMode.READONLY) throw IllegalStateException(String.format("Transactional '%s' is read-only; it cannot be used to make any changes.", this.txid))
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be used to make any changes.", this.txid))

            val inserted = ((this.map as MutableMap<Long, T>).putIfAbsent(tid, value) == null)
            if (inserted) {
                this.status = TransactionStatus.DIRTY
            }
            return inserted
        }

        /**
         * Writers the provided value for the provided tuple ID if.
         *
         * @param tid The Tuple ID for which to insert a value.
         * @param value The value to insert.
         * @throws IllegalStateException If [Tx] is either read-only or has been closed.
         */
        @Synchronized
        fun write(tid: Long, value: T) {
            if (this.mode === AccessorMode.READONLY) throw IllegalStateException(String.format("Transactional '%s' is read-only; it cannot be used to make any changes.", this.txid))
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be used to make any changes.", this.txid))

            this.map[tid] = value
            this.status = TransactionStatus.DIRTY
        }

        /**
         * Removes the the entry identified by the provided tuple ID, if it exists.
         *
         * @param tid Tuple ID for which to return the entry.
         * @return Value of the entry that has been removed or null, if no such value exists.
         */
        @Synchronized
        fun remove(tid: Long): Optional<T> {
            if (this.mode === AccessorMode.READONLY) throw IllegalStateException(String.format("Transactional '%s' is read-only; it cannot be used to make any changes.", this.txid))
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be used to make any changes.", this.txid))

            val removed = this.map.remove(tid)
            if (removed != null) {
                this.status = TransactionStatus.DIRTY
            }
            return Optional.ofNullable(removed)
        }

        /**
         * Commits all changes that were made since the last commit. Causes the [Transaction] to complete and close.
         *
         * Only works, if [Tx] has been created in mode [AccessorMode.READWRITE_TX].
         * Otherwise, calling this method has no effect.
         */
        @Synchronized
        override fun commit() {
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be committed.", this.txid))
            if (this.status === TransactionStatus.DIRTY) {
                this.database.commit()
                this.close()
            }
        }

        /**
         * Rolls back all changes that were made since the last commit. Causes the [Transaction] to complete and close.
         *
         * Rollback only works, if [Tx] has been created in mode [AccessorMode.READWRITE_TX].
         * Otherwise, calling this method has no effect.
         */
        @Synchronized
        override fun rollback() {
            if (this.status === TransactionStatus.CLOSED) throw IllegalStateException(String.format("Transactional '%s' has been closed; it cannot be committed.", this.txid))
            if (this.status === TransactionStatus.DIRTY) {
                this.database.commit()
                this.close()
            }
        }

        /**
         * Closes the underlying [DB] and ends the transaction.
         */
        @Synchronized
        override fun close() {
            this.database.close()
            this.status = TransactionStatus.CLOSED
        }
    }
}


