package org.vitrivr.cottontail.dbms.execution.transactions

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
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
import org.vitrivr.cottontail.dbms.operations.Operation
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [TransactionImpl]s.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class TransactionManager(val executionManager: ExecutionManager, transactionTableSize: Int, val transactionHistorySize: Int, private val catalogue: DefaultCatalogue) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionManager::class.java)
    }

    /** Map of [TransactionImpl]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<TransactionImpl>(transactionTableSize, VERY_FAST_LOAD_FACTOR))

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
     * A concrete [TransactionImpl] used for executing a query.
     *
     * A [TransactionImpl] can be of different [TransactionType]s. Their execution semantics may differ slightly.
     *
     * @author Ralph Gasser
     * @version 1.4.0
     */
    inner class TransactionImpl(override val type: TransactionType) : LockHolder<DBO>(this@TransactionManager.tidCounter.getAndIncrement()), Transaction, TransactionContext {

        /** The [TransactionStatus] of this [TransactionImpl]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.IDLE
            private set

        /** The Xodus [Transaction] associated with this [TransactionContext]. */
        override val xodusTx: jetbrains.exodus.env.Transaction = this@TransactionManager.catalogue.environment.beginTransaction()

        /** Map of all [Tx] that have been created as part of this [TransactionImpl]. */
        private val txns: MutableMap<Name, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** A [Mutex] data structure used for synchronisation on the [TransactionImpl]. */
        private val mutex = Mutex()

        /** Number of [Tx] held by this [TransactionImpl]. */
        val numberOfTxs: Int
            get() = this.txns.size

        /** Timestamp of when this [TransactionImpl] was created. */
        val created = System.currentTimeMillis()

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

        /** Flag indicating, that this [Transaction] was used to write data. */
        override var readonly: Boolean = true
            private set

        init {
            /** Add this to transaction history and transaction table. */
            this@TransactionManager.transactions[this.txId] = this
            this@TransactionManager.transactionHistory.add(this)
            if (this@TransactionManager.transactionHistory.size >= this@TransactionManager.transactionHistorySize) {
                this@TransactionManager.transactionHistory.removeAt(0)
            }
        }

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [TransactionImpl] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        override fun getTx(dbo: DBO): Tx = this.txns.computeIfAbsent(dbo.name) {
            dbo.newTx(this)
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
         * Signals a [Operation.DataManagementOperation] to this [TransactionImpl].
         *
         * Implementing methods must process these [Operation.DataManagementOperation]s quickly, since they are usually
         * triggered during an ongoing transaction.
         *
         * @param action The [Operation.DataManagementOperation] that has been reported.
         */
        override fun signalEvent(action: Operation.DataManagementOperation) {
            /* ToDo: Do something with the events. */
        }

        /**
         * Schedules an [Operator] for execution in this [TransactionImpl] and blocks, until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        override fun execute(operator: Operator): Flow<Record>  = operator.toFlow(this).flowOn(this@TransactionManager.executionManager.queryDispatcher).onStart {
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
                    for ((i, txn) in this@TransactionImpl.txns.values.reversed().withIndex()) {
                        try {
                            txn.beforeCommit()
                        } catch (e: Throwable) {
                            LOGGER.error("An error occurred while preparing Tx $i (${txn.dbo.name}) of transaction ${this@TransactionImpl.txId} for commit. This is serious!", e)
                            throw e
                        }
                    }
                    commit = this@TransactionImpl.xodusTx.commit()
                    if (!commit) throw TransactionException.InConflict(this@TransactionImpl.txId)
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
                for ((i, txn) in this@TransactionImpl.txns.values.reversed().withIndex()) {
                    try {
                        txn.beforeRollback()
                    } catch (e: Throwable) {
                        LOGGER.error("An error occurred while rolling back Tx $i (${txn.dbo.name}) of transaction ${this@TransactionImpl.txId}. This is serious!", e)
                    }
                }
                this.xodusTx.abort()
            } finally {
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
            this@TransactionImpl.ended = System.currentTimeMillis()
            this@TransactionImpl.state = if (committed) {
                TransactionStatus.COMMIT
            } else {
                TransactionStatus.ROLLBACK
            }
            this@TransactionManager.transactions.remove(this@TransactionImpl.txId)
        }
    }
}