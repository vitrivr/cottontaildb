package org.vitrivr.cottontail.dbms.execution.transactions

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager.AbstractTransaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.server.Instance
import org.vitrivr.cottontail.utilities.extensions.write
import java.lang.ref.SoftReference
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.StampedLock
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashSet
import kotlin.coroutines.CoroutineContext

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [AbstractTransaction]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class TransactionManager(val instance: Instance) {

    companion object {
        /** [Logger] instance used by [TransactionManager]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(TransactionManager::class.java)
    }

    /** Map of [AbstractTransaction]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<AbstractTransaction>(this.instance.config.execution.transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** List of past [TransactionMetadata] objects. */
    private val transactionHistory: MutableList<TransactionMetadata> = Collections.synchronizedList(ArrayList(this.instance.config.execution.transactionHistorySize))

    /** Set of [TransactionObserver]s registered with this [TransactionManager]. */
    private val observers = Collections.synchronizedSet(ObjectOpenHashSet<TransactionObserver>())

    /** Internal counter to generate [TransactionId]s. Starts with 1 */
    private val tidCounter = AtomicLong(1L)

    /** This lock synchronises exclusive transactions. */
    private val exclusiveLock = StampedLock()

    /** The [LockManager] instance used by this [TransactionManager]. */
    internal val lockManager = LockManager<DBO>()

    /**
     * Returns the [AbstractTransaction] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [AbstractTransaction] for.
     * @return [AbstractTransaction] or null
     */
    operator fun get(txId: TransactionId): Transaction? = this.transactions[txId]

    /**
     * Returns a list of [TransactionMetadata] of all ongoing and past transactions.
     *
     * @return [List] of [TransactionMetadata]
     */
    fun history(): List<TransactionMetadata> {
        return this.transactionHistory + this.transactions.values.toList()
    }

    /**
     * Registers a [TransactionObserver] with this [TransactionManager].
     *
     * The mechanism makes sure, that all ongoing transactions finish before registration.
     *
     * @param observer [TransactionObserver] to register.
     * @return True on success, false otherwise.
     */
    fun register(observer: TransactionObserver) {
        this.observers.add(observer)
    }

    /**
     * De-registers the given [TransactionObserver] with this [TransactionManager].
     *
     * The mechanism makes sure, that all ongoing transactions finish before de-registration.
     *
     * @param observer [TransactionObserver] to de-register.
     * @return True on success, false otherwise.
     */
    fun deregister(observer: TransactionObserver) {
        this.observers.remove(observer)
    }

    /**
     * Computes a block of code with exclusive access to the database, i.e., making sure that no concurrent write transaction is running.
     *
     * @param callback The [callback] to execute.
     * @return [T]
     */
    fun <T> computeExclusively(callback: () -> T): T = this.exclusiveLock.write {
        callback()
    }

    /**
     * Starts a new [AbstractTransaction] through this [TransactionManager].
     *
     * @param type The [TransactionType] of the [TransactionMetadata] to start.
     * @return [AbstractTransaction]
     */
    fun startTransaction(type: TransactionType): Transaction = if (type.readonly) {
        ReadonlyTransaction(type)
    } else {
        ExclusiveTransaction(type)
    }

    /**
     * A concrete [AbstractTransaction] used for executing a query.
     *
     * A [AbstractTransaction] can be of different [TransactionType]s. Their execution semantics may differ slightly.
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    private abstract inner class AbstractTransaction(override val type: TransactionType): Transaction {

        /** The [TransactionId] assigned to this [AbstractTransaction]*/
        final override val transactionId = this@TransactionManager.tidCounter.getAndIncrement()

        /** The [TransactionStatus] of this [AbstractTransaction]. */
        @Volatile
        final override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** Reference to the enclosing [TransactionManager]. */
        override val manager: TransactionManager
            get() = this@TransactionManager

        /**
         * A [MutableMap] of all [TransactionObserver] and the [Event]s that were collected for them.
         *
         * Since a lot of [Event]s can build-up during a [TransactionMetadata], the [MutableList] is wrapped in a [SoftReference],
         * which can be claimed by the garbage collector. Such a situation will result in failure to notify observers. Consequently,
         * the channel provided by this facility must be considered unreliable!
         */
        private val localObservers: MutableMap<TransactionObserver, SoftReference<MutableList<Event>>> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** A [LinkedHashSet] of all [SubTransaction] instances associated with this [AbstractTransaction]. */
        private val subTransactions = LinkedHashSet<SubTransaction>()

        /** A [Mutex] data structure used for synchronisation on the [AbstractTransaction]. */
        private val mutex = Mutex()

        /** A [HashSet] containing ALL [CoroutineContext] instances associated with execution in this [AbstractTransaction], */
        private val activeContexts = HashSet<CoroutineContext>()

        /** Timestamp of when this [AbstractTransaction] was created. */
        final override val created: Long = System.currentTimeMillis()

        /** Timestamp of when this [AbstractTransaction] was either COMMITTED or ABORTED. */
        final override var ended: Long? = null
            private set

        /** Number of queries executed successfully in this [AbstractTransaction]. */
        @Volatile
        final override var numberOfSuccess: Int = 0
            private set

        /** Number of queries executed with an error in this [AbstractTransaction]. */
        @Volatile
        final override var numberOfError: Int = 0
            private set

        /** Number of ongoing queries in this [AbstractTransaction]. */
        final override val numberOfOngoing: Int
            get() = this.activeContexts.size

        /** The number of available workers for query execution. */
        override val availableQueryWorkers: Int
            get() = this@TransactionManager.instance.executor.availableQueryWorkers()

        /** The number of available workers for intra-query execution. */
        override val availableIntraQueryWorkers: Int
            get() = this@TransactionManager.instance.executor.availableIntraQueryWorkers()

        init {
            /** Add this transaction to the manager's transaction table. */
            this@TransactionManager.transactions[this.transactionId] = this

            /**
             * Create transaction, local snapshot of the registered transaction observers.
             *
             * Observers registered after the transaction has started, are considered! This is a design choice!
             */
            for (observer in this@TransactionManager.observers) {
                this.localObservers[observer] = SoftReference(LinkedList<Event>())
            }
        }

        /**
         * Signals an [Event] to this [AbstractTransaction].
         *
         * Those [Event]s are stored, if any of the registered [localObservers]s signal interest in the [Event].
         *
         * @param event The [Event] that has been reported.
         */
        override fun signalEvent(event: Event) {
            for ((observer, listRef) in this.localObservers) {
                val list = listRef.get()
                if (list != null && observer.isRelevant(event)) {
                    list.add(event)
                }
            }
        }

        /**
         * Registers a [SubTransaction] with this [Transaction].
         *
         * @param subTransaction The [SubTransaction] to register.
         */
        override fun registerSubtransaction(subTransaction: SubTransaction) {
            this.subTransactions.add(subTransaction)
        }

        /**
         * Schedules an [Operator] for execution in this [AbstractTransaction] and blocks, until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        override fun execute(operator: Operator): Flow<Tuple> = operator.toFlow().flowOn(this@TransactionManager.instance.executor.queryDispatcher).onStart {
            this@AbstractTransaction.mutex.withLock {  /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                check(this@AbstractTransaction.state.canExecute) {
                    "Cannot start execution of transaction ${this@AbstractTransaction.transactionId} because it is in the wrong state (s = ${this@AbstractTransaction.state})."
                }
                this@AbstractTransaction.state = TransactionStatus.RUNNING
                this@AbstractTransaction.activeContexts.add(currentCoroutineContext())
            }
        }.onCompletion {
            this@AbstractTransaction.mutex.withLock { /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                /* Remove context. */
                this@AbstractTransaction.activeContexts.remove(currentCoroutineContext())

                /* Update statistics. */
                if (it == null) {
                    this@AbstractTransaction.numberOfSuccess += 1
                } else {
                    this@AbstractTransaction.numberOfError += 1
                    this@AbstractTransaction.state = TransactionStatus.ERROR
                }

                /* Update state of transaction if necessary. */
                if (this@AbstractTransaction.state == TransactionStatus.RUNNING && this@AbstractTransaction.activeContexts.isEmpty()) {
                    this@AbstractTransaction.state = TransactionStatus.IDLE
                }
            }
        }.cancellable()

        /**
         * Commits this [AbstractTransaction] thus finalizing and persisting all operations executed so far.
         */
        override fun commit() = runBlocking {
            this@AbstractTransaction.mutex.withLock { /* Synchronise with ongoing COMMITS, ROLLBACKS or queries that are being scheduled. */
                if (!this@AbstractTransaction.state.canCommit)
                    throw TransactionException.Commit(this@AbstractTransaction.transactionId, "Unable to COMMIT because transaction is in wrong state (s = ${this@AbstractTransaction.state}).")
                this@AbstractTransaction.state = TransactionStatus.PREPARE
            }

            /* Phase 1: Reach consensus about global commit. */
            val commitable = this@AbstractTransaction.subTransactions.filterIsInstance<SubTransaction.WithCommit>()
            val canCommit = try {
                /* Phase 1: Issue 'PREPARE TO COMMIT' Execute commit finalization. */
                commitable.all { it.prepareCommit() }
            } catch (e: Throwable) {
                LOGGER.error("Failed to commit transaction ${this@AbstractTransaction.transactionId} due to error in PREPARE phase. Aborting...", e)
                false
            }

            /* If the transaction cannot commit, perform a rollback. */
            if (!canCommit) {
                this@AbstractTransaction.state = TransactionStatus.ABORT
                this@AbstractTransaction.performRollback()
                return@runBlocking
            } else {
                this@AbstractTransaction.state = TransactionStatus.COMMIT
            }

            /* Phase 2: Issue global commit */
            try {
                commitable.forEach { it.commit() }
                this@AbstractTransaction.state = TransactionStatus.COMMITTED
            } catch (e: Throwable) {
                LOGGER.error("Failed to commit transaction ${this@AbstractTransaction.transactionId} due to error in GLOBAL COMMIT phase. Trying to abort. Data may be inconsistent", e)
            } finally {
                this@AbstractTransaction.finalizeTransaction()
            }
        }

        /**
         * Rolls back this [AbstractTransaction] thus reverting all operations executed so far.
         */
        override fun abort() = runBlocking {
            this@AbstractTransaction.mutex.withLock { /* Synchronise with ongoing COMMITS, ROLLBACKS or queries that are being scheduled. */
                if (!this@AbstractTransaction.state.canRollback)
                    throw TransactionException.Rollback(this@AbstractTransaction.transactionId, "Unable to ABORT because transaction is in wrong state (s = ${this@AbstractTransaction.state}).")
                this@AbstractTransaction.state = TransactionStatus.ABORT
            }
            this@AbstractTransaction.performRollback()
        }

        /**
         * Kills this [AbstractTransaction] interrupting all running queries and rolling it back.
         */
        override fun kill() = runBlocking {
            this@AbstractTransaction.mutex.withLock { /* Synchronise with ongoing COMMITS, ROLLBACKS or queries that are being scheduled. */
                /* Cancel all running queries. */
                if (this@AbstractTransaction.state === TransactionStatus.RUNNING) {
                    this@AbstractTransaction.activeContexts.forEach {
                        it.cancel(CancellationException("Transaction ${this@AbstractTransaction.transactionId} was killed by user."))
                    }
                }
                this@AbstractTransaction.state = TransactionStatus.ABORT
            }

            /* Wait for all queries to be cancelled. */
            while (this@AbstractTransaction.activeContexts.isNotEmpty()) {
                delay(250)
            }

            /* Perform actual rollback. */
            this@AbstractTransaction.performRollback()
        }

        /**
         * Actually performs transaction rollback; this method is not synchronized!
         */
        private fun performRollback() {
            require(this.state == TransactionStatus.ABORT) { "Cannot perform rollback of transaction ${this.transactionId} because it is in wrong state (s = ${this.state})." }
            try {
                for (txn in this@AbstractTransaction.subTransactions.filterIsInstance<SubTransaction.WithAbort>()) {
                    txn.abort()
                }
                this.state = TransactionStatus.ABORTED
            } catch (e: Throwable) {
                this.state = TransactionStatus.ERROR
                LOGGER.error("Failed to rollback transaction ${this@AbstractTransaction.transactionId} due to error in ABORT phase.", e)
            } finally {
                this@AbstractTransaction.finalizeTransaction()
            }
        }

        /**
         * Finalizes the state of this [AbstractTransaction].
         */
        protected open fun finalizeTransaction() {
            this@AbstractTransaction.subTransactions.removeAll {
                if (it is SubTransaction.WithFinalization) {
                    it.finalize(this.state == TransactionStatus.COMMITTED)
                }
                true
            }
            this@AbstractTransaction.ended = System.currentTimeMillis()

            /* Remove this transaction from the list of ongoing transactions. */
            this@TransactionManager.transactions.remove(this@AbstractTransaction.transactionId)

            /* Add this transaction to the transaction history. */
            this@TransactionManager.transactionHistory.add(FinishedTransaction(this))
            if (this@TransactionManager.transactionHistory.size > this@TransactionManager.instance.config.execution.transactionHistorySize) {
                this@TransactionManager.transactionHistory.removeAt(0)
            }

            /* Notify observers. */
            this.notifyObservers()
        }

        /**
         * Notifies all [TransactionObserver]s about a successful commit.
         */
        private fun notifyObservers(){
            for ((observer, listRef) in this.localObservers) {
                val list = listRef.get()
                if (list != null) {
                    observer.onCommit(this.transactionId, list)    /* Signal COMMIT to local observers. */
                } else {
                    observer.onDeliveryFailure(this.transactionId) /* Signal DELIVERY FAILURE to local observers. */
                }
            }

            /* Clear local observers to release memory. */
            this.localObservers.clear()
        }
    }

    /**
     * A read-only [Transaction].
     */
    private inner class ReadonlyTransaction(type: TransactionType): AbstractTransaction(type) {
        init {
            require(type.readonly) { "Unsupported transaction type $type for read-only transaction." }
        }
    }

    /**
     * An exclusive (write) [Transaction].
     */
    private inner class ExclusiveTransaction(type: TransactionType): AbstractTransaction(type) {

        init {
            require(type.exclusive) { "Unsupported transaction type $type for read-only transaction." }
        }

        /** An [ExclusiveTransaction] acquires an exclusive lock on the surrounding [TransactionManager]. */
        private val stamp = this@TransactionManager.exclusiveLock.tryWriteLock(this@TransactionManager.instance.config.execution.transactionTimeoutMs, TimeUnit.MILLISECONDS)

        init {
            if (this.stamp == -1L) {
                throw TransactionException.Timeout(this.transactionId)
            }
        }

        /**
         * The [ExclusiveTransaction] relinquishes its exclusive lock when it is finalized.
         */
        override fun finalizeTransaction() {
            this@TransactionManager.exclusiveLock.unlock(this.stamp)
            super.finalizeTransaction()
        }
    }
}