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
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager.AbstractTransaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.utilities.extensions.write
import java.lang.ref.SoftReference
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.StampedLock
import kotlin.coroutines.CoroutineContext

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [AbstractTransaction]s.
 *
 * @author Ralph Gasser
 * @version 1.9.0
 */
class TransactionManager(val executionManager: ExecutionManager, val config: Config) {
    /** Map of [AbstractTransaction]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<AbstractTransaction>(this.config.execution.transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** List of past [TransactionMetadata] objects. */
    private val transactionHistory: MutableList<TransactionMetadata> = Collections.synchronizedList(ArrayList(this.config.execution.transactionHistorySize))

    /** Set of [TransactionObserver]s registered with this [TransactionManager]. */
    private val observers = Collections.synchronizedSet(ObjectOpenHashSet<TransactionObserver>())

    /** Internal counter to generate [TransactionId]s. Starts with 1 */
    private val tidCounter = AtomicLong(1L)

    /** This lock synchronises exclusive transactions. */
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
     * Attempts to shut down this [TransactionManager].
     */
    fun shutdown() {
        this.vfs.shutdown()
        this.environment.close()
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

        /** Map of all [Tx] that have been created as part of this [AbstractTransaction]. */
        private val txns: MutableMap<Name, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** A [Mutex] data structure used for synchronisation on the [AbstractTransaction]. */
        private val mutex = Mutex()

        /** A [HashSet] containing ALL [CoroutineContext] instances associated with execution in this [AbstractTransaction], */
        private val activeContexts = HashSet<CoroutineContext>()

        /** Timestamp of when this [AbstractTransaction] was created. */
        final override val created: Long
            get() = this.xodusTx.startTime

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
            get() = this@TransactionManager.executionManager.availableQueryWorkers()

        /** The number of available workers for intra-query execution. */
        override val availableIntraQueryWorkers: Int
            get() = this@TransactionManager.executionManager.availableIntraQueryWorkers()

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
         * Caches a [Tx] in this [TransactionManager.AbstractTransaction]s cache.
         *
         * @param tx The [Tx] to cache
         * @return True if [Tx] was cached.
         */
        override fun cacheTx(tx: Tx): Boolean = this.txns.putIfAbsent(tx.dbo.name, tx) != null

        /**
         * Tries to retrieve a cached [Tx] from this [TransactionManager.AbstractTransaction]s cache.
         *
         * @param dbo The [DBO] to obtain the [Tx] for.
         * @return The [Tx] or null
         */
        @Suppress("UNCHECKED_CAST")
        override fun <T : Tx> getCachedTxForDBO(dbo: DBO): T? = this.txns[dbo.name] as T?

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
         * Schedules an [Operator] for execution in this [AbstractTransaction] and blocks, until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        override fun execute(operator: Operator): Flow<Tuple> = operator.toFlow().flowOn(this@TransactionManager.executionManager.queryDispatcher).onStart {
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
                this@AbstractTransaction.state = TransactionStatus.FINALIZING

                try {
                    /* Execute commit finalization. */
                    for (txn in this@AbstractTransaction.txns.values.filterIsInstance<Tx.WithCommitFinalization>()) {
                        txn.beforeCommit()
                    }

                    /* Execute actual commit. */
                    if (this@AbstractTransaction.xodusTx.isIdempotent) {
                        this@AbstractTransaction.xodusTx.abort()
                    } else if (!this@AbstractTransaction.xodusTx.commit()) {
                        throw TransactionException.InConflict(this@AbstractTransaction.transactionId)
                    }
                    this@AbstractTransaction.state = TransactionStatus.COMMIT
                } catch (e: Throwable) {
                    this@AbstractTransaction.xodusTx.abort()
                    this@AbstractTransaction.state = TransactionStatus.ROLLBACK
                    throw e
                } finally {
                    this@AbstractTransaction.finalizeTransaction()
                    if (this@AbstractTransaction.state === TransactionStatus.COMMIT) {
                        this@AbstractTransaction.notifyObservers()
                    }
                }
            }
        }

        /**
         * Rolls back this [AbstractTransaction] thus reverting all operations executed so far.
         */
        override fun rollback() = runBlocking {
            this@AbstractTransaction.mutex.withLock {
                if (!this@AbstractTransaction.state.canRollback)
                    throw TransactionException.Rollback(this@AbstractTransaction.transactionId, "Unable to ROLLBACK because transaction is in wrong state (s = ${this@AbstractTransaction.state}).")
                this@AbstractTransaction.performRollback()
            }
        }

        /**
         * Kills this [AbstractTransaction] interrupting all running queries and rolling it back.
         */
        override fun kill() = runBlocking {
            this@AbstractTransaction.mutex.withLock {
                /* Cancel all running queries. */
                if (this@AbstractTransaction.state === TransactionStatus.RUNNING) {
                    this@AbstractTransaction.activeContexts.forEach {
                        it.cancel(CancellationException("Transaction ${this@AbstractTransaction.transactionId} was killed by user."))
                    }
                }
                this@AbstractTransaction.state = TransactionStatus.FINALIZING
            }

            /* Wait for all queries to be cancelled. */
            while (this@AbstractTransaction.activeContexts.isNotEmpty()) {
                delay(250)
            }

            /* Perform actual rollback. */
            this@AbstractTransaction.performRollback()
        }

        /**
         * Actually performs transaction rollback.
         */
        private fun performRollback() {
            this@AbstractTransaction.state = TransactionStatus.FINALIZING
            try {
                for (txn in this@AbstractTransaction.txns.values.filterIsInstance<Tx.WithRollbackFinalization>()) {
                    txn.beforeRollback()
                }
                this.xodusTx.abort()
                this.state = TransactionStatus.ROLLBACK
            } finally {
                this@AbstractTransaction.finalizeTransaction()
            }
        }

        /**
         * Finalizes the state of this [AbstractTransaction].
         */
        protected open fun finalizeTransaction() {
            this@AbstractTransaction.txns.clear()
            this@AbstractTransaction.ended = System.currentTimeMillis()

            /* Remove this transaction from the list of ongoing transactions. */
            this@TransactionManager.transactions.remove(this@AbstractTransaction.transactionId)

            /* Add this transaction to the transaction history. */
            this@TransactionManager.transactionHistory.add(FinishedTransaction(this))
            if (this@TransactionManager.transactionHistory.size > this@TransactionManager.config.execution.transactionHistorySize) {
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

    /**
     * A read-only [Transaction].
     */
    private inner class ReadonlyTransaction(type: TransactionType): AbstractTransaction(type) {
        init {
            require(type.readonly) { "Unsupported transaction type $type for read-only transaction." }
        }

        /** The Xodus [Transaction] backing this [ReadonlyTransaction]. */
        override val xodusTx = this@TransactionManager.environment.beginReadonlyTransaction()
    }

    /**
     * An exclusive (write) [Transaction].
     */
    private inner class ExclusiveTransaction(type: TransactionType): AbstractTransaction(type) {

        init {
            require(type.exclusive) { "Unsupported transaction type $type for read-only transaction." }
        }

        /** An [ExclusiveTransaction] acquires an exclusive lock on the surrounding [TransactionManager]. */
        private val stamp = this@TransactionManager.exclusiveLock.tryWriteLock(this@TransactionManager.config.execution.transactionTimeoutMs, TimeUnit.MILLISECONDS)

        init {
            if (this.stamp == -1L) {
                throw TransactionException.Timeout(this.transactionId)
            }
        }

        /** The Xodus [Transaction] backing this [ExclusiveTransaction]. */
        override val xodusTx = this@TransactionManager.environment.beginExclusiveTransaction()

        /**
         * The [ExclusiveTransaction] relinquishes its exclusive lock when it is finalized.
         */
        override fun finalizeTransaction() {
            this@TransactionManager.exclusiveLock.unlock(this.stamp)
            super.finalizeTransaction()
        }
    }
}