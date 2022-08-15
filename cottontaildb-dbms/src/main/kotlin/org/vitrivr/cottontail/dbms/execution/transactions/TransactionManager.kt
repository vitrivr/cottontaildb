package org.vitrivr.cottontail.dbms.execution.transactions

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.locking.Lock
import org.vitrivr.cottontail.dbms.execution.locking.LockHolder
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.locking.LockMode
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager.TransactionImpl
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx
import java.lang.ref.SoftReference
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [TransactionImpl]s.
 *
 * @author Ralph Gasser
 * @version 1.7.0
 */
class TransactionManager(val executionManager: ExecutionManager, transactionTableSize: Int, val transactionHistorySize: Int, private val catalogue: DefaultCatalogue) {
    /** Map of [TransactionImpl]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<TransactionImpl>(transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** Set of [TransactionObserver]s registered with this [TransactionManager]. */
    private val observers = Collections.synchronizedSet(ObjectOpenHashSet<TransactionObserver>())

    /** Internal counter to generate [TransactionId]s. Starts with 1 */
    private val tidCounter = AtomicLong(1L)

    /** The [LockManager] instance used by this [TransactionManager]. */
    internal val lockManager = LockManager<DBO>()

    /** List of ongoing or past transactions (limited to [transactionHistorySize] entries). */
    internal val transactionHistory: MutableList<TransactionImpl> = Collections.synchronizedList(ArrayList(this.transactionHistorySize))

    /**
     * Returns the [TransactionImpl] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [TransactionImpl] for.
     * @return [TransactionImpl] or null
     */
    operator fun get(txId: TransactionId): TransactionImpl? = this.transactions[txId]

    /**
     * Registers a [TransactionObserver] with this.
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
     * @param observer [TransactionObserver] to de-register.
     * @return True on success, false otherwise.
     */
    fun deregister(observer: TransactionObserver) {
        this.observers.remove(observer)
    }

    /**
     * A concrete [TransactionImpl] used for executing a query.
     *
     * A [TransactionImpl] can be of different [TransactionType]s. Their execution semantics may differ slightly.
     *
     * @author Ralph Gasser
     * @version 1.5.0
     */
    inner class TransactionImpl(override val type: TransactionType) : LockHolder<DBO>(this@TransactionManager.tidCounter.getAndIncrement()), Transaction, TransactionContext {

        /** The [TransactionStatus] of this [TransactionImpl]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** The Xodus [Transaction] associated with this [TransactionContext]. */
        override val xodusTx: jetbrains.exodus.env.Transaction = when {
            this.type.readonly -> this@TransactionManager.catalogue.environment.beginReadonlyTransaction()   /* Read-only transaction. */
            this.type.exclusive -> this@TransactionManager.catalogue.environment.beginExclusiveTransaction() /* Exclusive write transaction. */
            else -> this@TransactionManager.catalogue.environment.beginTransaction()                         /* Optimistic write transaction. */
        }

        /**
         *
         */
        override fun cacheTxForDBO(tx: Tx): Boolean
            = this.txns.putIfAbsent(tx.dbo.name, tx) != null

        /**
         * Tries to retrieve a cached [Tx] from this [TransactionManager.TransactionImpl]s cache.
         *
         * @param dbo The [DBO] to obtain the [Tx] for.
         * @return The [Tx] or null
         */
        override fun <T : Tx> getCachedTxForDBO(dbo: DBO): T?
            = this.txns[dbo.name] as T?

        /**
         * A [MutableMap] of all [TransactionObserver] and the [Event]s that were collected for them.
         *
         * Since a lot of [Event]s can build-up during a [Transaction], the [MutableList] is wrapped in a [SoftReference],
         * which can be claimed by the garbage collector. Such a situation will result in failure to notify observers. Consequently,
         * the channel provided by this facility must be considered unreliable!
         */
        private val localObservers: MutableMap<TransactionObserver, SoftReference<MutableList<Event>>> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** Map of all [Tx] that have been created as part of this [TransactionImpl]. */
        private val txns: MutableMap<Name, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** List of all [Tx.WithCommitFinalization] that must be notified before commit. */
        private val notifyOnCommit: MutableList<Tx.WithCommitFinalization> = Collections.synchronizedList(LinkedList())

        /** List of all [Tx.WithRollbackFinalization] that must be notified before rollback. */
        private val notifyOnRollback: MutableList<Tx.WithRollbackFinalization> = Collections.synchronizedList(LinkedList())

        /** A [Mutex] data structure used for synchronisation on the [TransactionImpl]. */
        private val mutex = Mutex()

        /** Number of [Tx] held by this [TransactionImpl]. */
        val numberOfTxs: Int
            get() = this.txns.size

        /** Timestamp of when this [TransactionImpl] was created. */
        val created
            get() = this.xodusTx.startTime

        /** Timestamp of when this [TransactionImpl] was either COMMITTED or ABORTED. */
        var ended: Long? = null
            private set

        /** Number of queries executed successfully in this [TransactionImpl]. */
        @Volatile
        var numberOfSuccess: Int = 0
            private set

        /** Number of queries executed with an error in this [TransactionImpl]. */
        @Volatile
        var numberOfError: Int = 0
            private set

        /** A [HashSet] containing ALL [CoroutineContext] instances associated with execution in this [TransactionImpl], */
        private val activeContexts = HashSet<CoroutineContext>()

        /** Number of ongoing queries in this [TransactionImpl]. */
        val numberOfOngoing: Int
            get() = this.activeContexts.size

        /** The number of available workers for query execution. */
        override val availableQueryWorkers: Int
            get() = this@TransactionManager.executionManager.availableQueryWorkers()

        /** The number of available workers for intra-query execution. */
        override val availableIntraQueryWorkers: Int
            get() = this@TransactionManager.executionManager.availableIntraQueryWorkers()

        init {
            /** Add this to transaction history and transaction table. */
            this@TransactionManager.transactions[this.txId] = this
            this@TransactionManager.transactionHistory.add(this)
            if (this@TransactionManager.transactionHistory.size >= this@TransactionManager.transactionHistorySize) {
                this@TransactionManager.transactionHistory.removeAt(0)
            }

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
         * Tries to acquire a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
         * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to request the lock for.
         * @param mode The desired [LockMode]
         */
        override fun requestLock(dbo: DBO, mode: LockMode) = this@TransactionManager.lockManager.lock(this, dbo, mode)

        /**
         * Signals an [Event] to this [TransactionImpl].
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
         * Schedules an [Operator] for execution in this [TransactionImpl] and blocks, until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        override fun execute(operator: Operator): Flow<Record>  = operator.toFlow().flowOn(this@TransactionManager.executionManager.queryDispatcher).onStart {
            this@TransactionImpl.mutex.withLock {  /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                check(this@TransactionImpl.state.canExecute) {
                    "Cannot start execution of transaction ${this@TransactionImpl.txId} because it is in the wrong state (s = ${this@TransactionImpl.state})."
                }
                this@TransactionImpl.state = TransactionStatus.RUNNING
                this@TransactionImpl.activeContexts.add(currentCoroutineContext())
            }
        }.onCompletion {
            this@TransactionImpl.mutex.withLock { /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                this@TransactionImpl.activeContexts.remove(currentCoroutineContext())
                if (it == null) {
                    this@TransactionImpl.numberOfSuccess += 1
                    if (this@TransactionImpl.numberOfOngoing == 0 && this@TransactionImpl.state == TransactionStatus.RUNNING) {
                        this@TransactionImpl.state = TransactionStatus.IDLE
                    }
                } else {
                    this@TransactionImpl.numberOfError += 1
                    this@TransactionImpl.state = TransactionStatus.ERROR
                }
            }
        }.cancellable()

        /**
         * Commits this [TransactionImpl] thus finalizing and persisting all operations executed so far.
         */
        override fun commit() = runBlocking {
            this@TransactionImpl.mutex.withLock { /* Synchronise with ongoing COMMITS, ROLLBACKS or queries that are being scheduled. */
                if (!this@TransactionImpl.state.canCommit)
                    throw TransactionException.Commit(this@TransactionImpl.txId, "Unable to COMMIT because transaction is in wrong state (s = ${this@TransactionImpl.state}).")
                this@TransactionImpl.state = TransactionStatus.FINALIZING
                var commit = false
                try {
                    for (txn in this@TransactionImpl.notifyOnCommit) {
                        txn.beforeCommit()
                    }
                    if (this@TransactionImpl.xodusTx.isReadonly) {
                        commit = true /* Xodus read-only transaction cannot be committed. */
                        this@TransactionImpl.xodusTx.abort()
                    } else {
                        commit = this@TransactionImpl.xodusTx.commit()
                        if (!commit) throw TransactionException.InConflict(this@TransactionImpl.txId)
                    }
                } catch (e: Throwable) {
                    this@TransactionImpl.xodusTx.abort()
                    throw e
                } finally {
                    this@TransactionImpl.finalize(commit)
                }
            }
        }

        /**
         * Rolls back this [TransactionImpl] thus reverting all operations executed so far.
         */
        override fun rollback() = runBlocking {
            this@TransactionImpl.mutex.withLock {
                if (!this@TransactionImpl.state.canRollback)
                    throw TransactionException.Rollback(this@TransactionImpl.txId, "Unable to ROLLBACK because transaction is in wrong state (s = ${this@TransactionImpl.state}).")
                this@TransactionImpl.performRollback()
            }
        }

        /**
         * Kills this [TransactionImpl] interrupting all running queries and rolling it back.
         */
        override fun kill() = runBlocking {
            this@TransactionImpl.mutex.withLock {
                if (this@TransactionImpl.state === TransactionStatus.RUNNING) {
                    this@TransactionImpl.activeContexts.forEach {
                        it.cancel(CancellationException("Transaction ${this@TransactionImpl.txId} was killed by user."))
                    }
                } else if (this@TransactionImpl.state.canRollback) {
                    this@TransactionImpl.performRollback()
                } else {
                    throw IllegalStateException( "Unable to kill transaction ${this@TransactionImpl.txId} because it is in wrong state (s = ${this@TransactionImpl.state}).")
                }
            }
        }

        /**
         * Actually performs transaction rollback.
         */
        private fun performRollback() {
            this@TransactionImpl.state = TransactionStatus.FINALIZING
            try {
                for (txn in this@TransactionImpl.notifyOnRollback) {
                    txn.beforeRollback()
                }
            } finally {
                this.xodusTx.abort()
                this@TransactionImpl.finalize(false)
            }
        }

        /**
         * Finalizes the state of this [TransactionImpl].
         *
         * @param committed True if [TransactionImpl] was committed, false otherwise.
         */
        private fun finalize(committed: Boolean) {
            this@TransactionImpl.allLocks().forEach { this@TransactionManager.lockManager.unlock(this@TransactionImpl, it.obj) }
            this@TransactionImpl.txns.clear()
            this@TransactionImpl.notifyOnCommit.clear()
            this@TransactionImpl.notifyOnRollback.clear()
            this@TransactionImpl.ended = System.currentTimeMillis()
            this@TransactionManager.transactions.remove(this@TransactionImpl.txId)
            if (committed) {
                this@TransactionImpl.state = TransactionStatus.COMMIT
                for ((observer, listRef) in this.localObservers) {
                    val list = listRef.get()
                    if (list != null) {
                        observer.onCommit(this.txId, list)    /* Signal COMMIT to local observers. */
                    } else {
                        observer.onDeliveryFailure(this.txId) /* Signal DELIVERY FAILURE to local observers. */
                    }
                }
            } else {
                this@TransactionImpl.state = TransactionStatus.ROLLBACK
            }

            /* Clear local observers to release memory. */
            this.localObservers.clear()
        }
    }
}