package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.exceptions.TransactionException

import java.util.*
import java.util.concurrent.locks.StampedLock


/**
 * Represents an index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to index one to many
 * [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return [Recordset]s.
 *
 * Calling the default constructor for an [Index] should open that [Index]. In order to initialize or rebuild it, a call to
 * [Index.update] us necessary. For concurrency reason, that call can only be issued through an [IndexTransaction].
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.1
 */
abstract class Index : DBO {

    /** An internal lock that is used to synchronize structural changes to an [Index] (e.g. closing or deleting) with running [Index.Tx]. */
    protected val globalLock = StampedLock()

    /** An internal lock that is used to synchronize concurrent read & write access to this [Index] by different [Index.Tx]. */
    protected val txLock = StampedLock()

    /** Reference to the [Entity], this index belongs to. */
    abstract override val parent: Entity

    /** The [ColumnDef] that are covered (i.e. indexed) by this [Index]. */
    abstract val columns: Array<ColumnDef<*>>

    /**
     * The [ColumnDef] that are produces by this [Index]. They may differ from the indexed columns, since some [Index] implementations
     * only return a tuple ID OR because some implementations may add some kind of score.
     */
    abstract val produces: Array<ColumnDef<*>>

    /** The type of [Index]. */
    abstract val type: IndexType

    /**
     * Checks if this [Index] can process the provided [BooleanPredicate] and returns true if so and false otherwise.
     *
     * @param predicate [BooleanPredicate] to check.
     * @return True if [BooleanPredicate] can be processed, false otherwise.
     */
    abstract fun canProcess(predicate: BooleanPredicate): Boolean

    /**
     * Calculates the cost estimate if this [Index] processing the provided [BooleanPredicate].
     *
     * @param predicate [BooleanPredicate] to check.
     * @return Cost estimate for the [BooleanPredicate]
     */
    abstract fun cost(predicate: BooleanPredicate): Float

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [Index].
     */
    @Synchronized
    protected fun finalize() {
        this.close()
    }

    /**
     * (Re-)builds the [Index]. Invoking this method should rebuild the [Index] immediately, without the
     * need to commit (i.e. commit actions must take place inside).
     *
     * This is an internal method! External invocation is only possible through a [Index.Tx] object.
     */
    protected abstract fun rebuild()

    /**
     * Performs a lookup through this [Index] and returns [Recordset]. This is an internal method! External
     * invocation is only possible through a [Index.Tx] object.
     *
     * This is the minimal method any [Index] implementation must support.
     *
     * @param predicate The [BooleanPredicate] to perform the lookup.
     * @return The resulting [Recordset].
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    protected abstract fun filter(predicate: BooleanPredicate): Recordset

    /**
     * Applies the given action to all the [Index] entries that match the given [Predicate]. This is an internal method!
     * External invocation is only possible through a [Index.Tx] object.
     *
     * The default implementation simply performs a lookup and applies the action in memory. More efficient implementations
     * are possible in many cases.
     *
     * @param predicate The [BooleanPredicate] to perform the lookup.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    protected open fun forEach(predicate: BooleanPredicate, action: (Record) -> Unit) = this.filter(predicate).forEach(action)

    /**
     * Applies the given action to all the [Index] entries that match the given [Predicate] and are located in the given range.
     * This is an internal method! External invocation is only possible through a [Index.Tx] object.
     *
     * The default implementation simply performs a lookup using the predicate and applies the action in memory. More efficient
     * implementations are possible in many cases.
     *
     * @param from The tuple ID to scan from.
     * @param to The tuple ID to scan to.
     * @param predicate The [BooleanPredicate] to perform the lookup.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    protected open fun forEach(from: Long, to: Long, predicate: BooleanPredicate, action: (Record) -> Unit) = this.filter(predicate).forEach(from, to, action)

    /**
     * Applies the given mapping function to all the [Index] entries that match the given [Predicate]. This is an internal
     * method! External invocation is only possible through a [Index.Tx] object.
     *
     * The default implementation simply performs a lookup and applies the mapping function in memory.
     * More efficient implementations are possible in many cases.
     *
     * @param predicate The [BooleanPredicate] to perform the lookup.
     * @param action The action that should be applied.

     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    protected open fun <R> map(predicate: BooleanPredicate, action: (Record) -> R): Collection<R> = this.filter(predicate).map(action)

    /**
     * Applies the given mapping function to all the [Index] entries that match the given [Predicate] and are located in the given range.
     * This is an internal method! External invocation is only possible through a [Index.Tx] object.
     *
     * The default implementation simply performs a lookup and applies the mapping function in memory.
     * More efficient implementations are possible in many cases.
     *
     * @param from The tuple ID to scan from.
     * @param to The tuple ID to scan to.
     * @param predicate The [BooleanPredicate] to perform the lookup.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    protected open fun <R> map(from: Long, to: Long, predicate: BooleanPredicate, action: (Record) -> R): Collection<R> = this.filter(predicate).map(from, to, action)

    /**
     * A [Transaction] that affects this [Index].
     */
    inner class Tx constructor(override val readonly: Boolean, override val tid: UUID = UUID.randomUUID()): IndexTransaction {

        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** Tries to acquire a global read-lock on the [MapDBColumn]. */
        init {
            if (this@Index.closed) {
                throw TransactionException.TransactionDBOClosedException(tid)
            }
        }

        /** Obtains a global (non-exclusive) read-lock on [Index]. Prevents enclosing [Index] from being closed while this [Index.Tx] is still in use. */
        private val globalStamp = this@Index.globalLock.readLock()

        /** Obtains tx lock on [Index]. Prevents concurrent read & write access to the enclosing [Index]. */
        private val txStamp = if (this.readonly) {
            this@Index.txLock.readLock()
        } else {
            this@Index.txLock.writeLock()
        }

        /** The [ColumnDef]s covered by the [Index] that underpins this [IndexTransaction]. */
        override val columns: Array<ColumnDef<*>>
            get() = this@Index.columns

        /** The [ColumnDef]s returned by the [Index] that underpins this [IndexTransaction]. */
        override val produces: Array<ColumnDef<*>>
            get() = this@Index.produces

        /** he [IndexType] of the [Index] that underpins this [IndexTransaction]. */
        override val type: IndexType
            get() = this@Index.type

        /**
         * Checks if this [IndexTransaction] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: BooleanPredicate): Boolean = this@Index.canProcess(predicate)

        /**
         * (Re-)builds the underlying [Index].
         */
        override fun rebuild() {
            this.acquireWriteLock()
            this@Index.rebuild()
        }

        /**
         * Performs a lookup through the underlying [Index] and returns a [Recordset].
         *
         * @param predicate The [BooleanPredicate] to perform the lookup.
         * @return The resulting [Recordset].
         *
         * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
         */
        override fun filter(predicate: BooleanPredicate): Recordset = this@Index.filter(predicate)

        /**
         * Applies the given action to all the [Index] entries that match the given [Predicate].
         *
         * @param predicate The [BooleanPredicate] to perform the lookup.
         * @param action The action that should be applied.
         *
         * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
         */
        override fun forEach(predicate: BooleanPredicate, action: (Record) -> Unit) =this@Index.forEach(predicate, action)

        /**
         * Applies the given action to all the [Index] entries that match the given [Predicate] and are within the given range.
         *
         * @param from The tuple ID to scan from.
         * @param to The tuple ID to scan to.
         * @param predicate The [BooleanPredicate] to perform the lookup.
         * @param action The action that should be applied.
         *
         * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
         */
        override fun forEach(from: Long, to: Long, predicate: BooleanPredicate, action: (Record) -> Unit) = this@Index.forEach(from, to, predicate, action)

        /**
         * Applies the given mapping function to all the [Index] entries that match the given [Predicate].
         *
         * @param predicate The [BooleanPredicate] to perform the lookup.
         * @param action The action that should be applied.
         *
         * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
         */
        override fun <R> map(predicate: BooleanPredicate, action: (Record) -> R): Collection<R> = this@Index.map(predicate, action)

        /**
         * Applies the given mapping function to all the [Index] entries that match the given [Predicate] and are within the given range.
         *
         * @param from The tuple ID to scan from.
         * @param to The tuple ID to scan to.
         * @param predicate The [BooleanPredicate] to perform the lookup.
         * @param action The action that should be applied.
         *
         * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
         */
        override fun <R> map(from: Long, to: Long, predicate: BooleanPredicate, action: (Record) -> R): Collection<R> = this@Index.map(from, to, predicate, action)

        /** Has no effect since updating an [Index] takes immediate effect. */
        override fun commit() {}

        /** Has no effect since updating an [Index] takes immediate effect. */
        override fun rollback() {}

        /**
         * Closes this [Index.Tx] and releases the global lock. Closed [Entity.Tx] cannot be used anymore!
         */
        @Synchronized
        override fun close() {
            if (this.status != TransactionStatus.CLOSED) {
                this.status = TransactionStatus.CLOSED
                this@Index.txLock.unlock(this.txStamp)
                this@Index.globalLock.unlockRead(this.globalStamp)
            }
        }

        /**
         * Tries to acquire a write-lock. If method fails, an exception will be thrown
         */
        @Synchronized
        private fun acquireWriteLock() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }
    }
}