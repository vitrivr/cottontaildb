package org.vitrivr.cottontail.dbms.execution.transactions

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.vfs.ClusteringStrategy
import jetbrains.exodus.vfs.VfsConfig
import jetbrains.exodus.vfs.VirtualFileSystem
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.tuple.Tuple
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
import org.vitrivr.cottontail.utilities.extensions.write
import java.lang.ref.SoftReference
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.StampedLock
import kotlin.coroutines.CoroutineContext

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [TransactionImpl]s.
 *
 * @author Ralph Gasser
 * @version 1.9.0
 */
class TransactionManager(val executionManager: ExecutionManager, val config: Config) {
    /** Map of [TransactionImpl]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<TransactionImpl>(this.config.execution.transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** List of past [TransactionMetadata] objects. */
    private val transactionHistory: MutableList<TransactionMetadata> = Collections.synchronizedList(ArrayList(this.config.execution.transactionHistorySize))

    /** Set of [TransactionObserver]s registered with this [TransactionManager]. */
    private val observers = Collections.synchronizedSet(ObjectOpenHashSet<TransactionObserver>())

    /** Internal counter to generate [TransactionId]s. Starts with 1 */
    private val tidCounter = AtomicLong(1L)

    /** This lock synchronises exclusive transactions (TODO: More granularity). */
    private val exclusiveLock = StampedLock()

    /** The [LockManager] instance used by this [TransactionManager]. */
    internal val lockManager = LockManager<DBO>()

    /** The main Xodus [Environment] used by Cottontail DB. This is an internal variable and not part of the official interface. */
    internal val environment: Environment = Environments.newInstance(
        this.config.dataFolder().toFile(),
        this.config.xodus.toEnvironmentConfig()
    )

    /** The [VirtualFileSystem] used by this [TransactionManager]. */
    internal val vfs: VirtualFileSystem

    init {
        val tx = this.environment.beginExclusiveTransaction()
        try {
            /** Initialize virtual file system. */
            val config = VfsConfig()
            config.clusteringStrategy = ClusteringStrategy.QuadraticClusteringStrategy(65536)
            config.clusteringStrategy.maxClusterSize = 65536 * 16
            this.vfs = VirtualFileSystem(this.environment, config, tx)
            tx.commit()
        } catch (e: Throwable) {
            tx.abort()
            throw e
        }
    }

    /**
     * Returns the [Transaction] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [Transaction] for.
     * @return [Transaction] or null
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
     * Starts a new [Transaction] through this [TransactionManager].
     *
     * @param type The [TransactionType] of the [TransactionMetadata] to start.
     * @return [Transaction]
     */
    fun startTransaction(type: TransactionType): Transaction = TransactionImpl(type)


    /**
     * Attempts to shutdown this [TransactionManager].
     */
    fun shutdown() {
        this.vfs.shutdown()
        this.environment.close()
    }

    /**
     * A concrete [TransactionImpl] used for executing a query.
     *
     * A [TransactionImpl] can be of different [TransactionType]s. Their execution semantics may differ slightly.
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    private inner class TransactionImpl(override val type: TransactionType): LockHolder<DBO>(this@TransactionManager.tidCounter.getAndIncrement()), Transaction {

        /** The [TransactionStatus] of this [TransactionImpl]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** Reference to the enclosing [TransactionManager]. */
        override val manager: TransactionManager
            get() = this@TransactionManager

        /** The Xodus [TransactionMetadata] associated with this [Transaction]. */
        override val xodusTx: jetbrains.exodus.env.Transaction

        /**
         * A [MutableMap] of all [TransactionObserver] and the [Event]s that were collected for them.
         *
         * Since a lot of [Event]s can build-up during a [TransactionMetadata], the [MutableList] is wrapped in a [SoftReference],
         * which can be claimed by the garbage collector. Such a situation will result in failure to notify observers. Consequently,
         * the channel provided by this facility must be considered unreliable!
         */
        private val localObservers: MutableMap<TransactionObserver, SoftReference<MutableList<Event>>> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** Map of all [Tx] that have been created as part of this [TransactionImpl]. */
        private val txns: MutableMap<Name, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** A [Mutex] data structure used for synchronisation on the [TransactionImpl]. */
        private val mutex = Mutex()

        /** Timestamp of when this [TransactionImpl] was created. */
        override val created
            get() = this.xodusTx.startTime

        /** Timestamp of when this [TransactionImpl] was either COMMITTED or ABORTED. */
        override var ended: Long? = null
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
            /** Try to start transaction. */
            if (this.type.exclusive) {
                this.xodusTx = this@TransactionManager.environment.beginExclusiveTransaction()
            } else {
                this.xodusTx = this@TransactionManager.environment.beginReadonlyTransaction()
            }

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
         * Caches a [Tx] in this [TransactionManager.TransactionImpl]s cache.
         *
         * @param tx The [Tx] to cache
         * @return True if [Tx] was cached.
         */
        override fun cacheTx(tx: Tx): Boolean = this.txns.putIfAbsent(tx.dbo.name, tx) != null

        /**
         * Tries to retrieve a cached [Tx] from this [TransactionManager.TransactionImpl]s cache.
         *
         * @param dbo The [DBO] to obtain the [Tx] for.
         * @return The [Tx] or null
         */
        @Suppress("UNCHECKED_CAST")
        override fun <T : Tx> getCachedTxForDBO(dbo: DBO): T? = this.txns[dbo.name] as T?

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
        override fun execute(operator: Operator): Flow<Tuple>  = operator.toFlow().flowOn(this@TransactionManager.executionManager.queryDispatcher).onStart {
            this@TransactionImpl.mutex.withLock {  /* Update transaction state; synchronise with ongoing COMMITS or ROLLBACKS. */
                check(this@TransactionImpl.state.canExecute) {
                    "Cannot start execution of transaction ${this@TransactionImpl.transactionId} because it is in the wrong state (s = ${this@TransactionImpl.state})."
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
                    throw TransactionException.Commit(this@TransactionImpl.transactionId, "Unable to COMMIT because transaction is in wrong state (s = ${this@TransactionImpl.state}).")
                this@TransactionImpl.state = TransactionStatus.FINALIZING

                try {
                    /* Execute commit finalization. */
                    for (txn in this@TransactionImpl.txns.values.filterIsInstance<Tx.WithCommitFinalization>()) {
                        txn.beforeCommit()
                    }
                    if (this@TransactionImpl.xodusTx.isReadonly) {
                        this@TransactionImpl.xodusTx.abort()
                    } else {
                        if (!this@TransactionImpl.xodusTx.commit())
                            throw TransactionException.InConflict(this@TransactionImpl.transactionId)
                    }
                    this@TransactionImpl.state = TransactionStatus.COMMIT
                    this@TransactionImpl.notifyObservers()
                } catch (e: Throwable) {
                    this@TransactionImpl.xodusTx.abort()
                    this@TransactionImpl.state = TransactionStatus.ROLLBACK
                    throw e
                } finally {
                    this@TransactionImpl.finalize()
                }
            }
        }

        /**
         * Rolls back this [TransactionImpl] thus reverting all operations executed so far.
         */
        override fun rollback() = runBlocking {
            this@TransactionImpl.mutex.withLock {
                if (!this@TransactionImpl.state.canRollback)
                    throw TransactionException.Rollback(this@TransactionImpl.transactionId, "Unable to ROLLBACK because transaction is in wrong state (s = ${this@TransactionImpl.state}).")
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
                        it.cancel(CancellationException("Transaction ${this@TransactionImpl.transactionId} was killed by user."))
                    }
                } else if (this@TransactionImpl.state.canRollback) {
                    this@TransactionImpl.performRollback()
                } else {
                    throw IllegalStateException( "Unable to kill transaction ${this@TransactionImpl.transactionId} because it is in wrong state (s = ${this@TransactionImpl.state}).")
                }
            }
        }

        /**
         * Actually performs transaction rollback.
         */
        private fun performRollback() {
            this@TransactionImpl.state = TransactionStatus.FINALIZING
            try {
                for (txn in this@TransactionImpl.txns.values.filterIsInstance<Tx.WithRollbackFinalization>()) {
                    txn.beforeRollback()
                }
                this.xodusTx.abort()
                this.state = TransactionStatus.ROLLBACK
            } finally {
                this@TransactionImpl.finalize()
            }
        }

        /**
         * Finalizes the state of this [TransactionImpl].
         */
        private fun finalize() {
            this@TransactionImpl.txns.clear()
            this@TransactionImpl.ended = System.currentTimeMillis()
            this@TransactionManager.transactions.remove(this@TransactionImpl.transactionId)

            /* Add this transaction to the transaction history. */
            this@TransactionManager.transactionHistory.add(FinishedTransaction(this))
            if (this@TransactionManager.transactionHistory.size >= this@TransactionManager.config.execution.transactionHistorySize) {
                this@TransactionManager.transactionHistory.removeAt(0)
            }
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
}