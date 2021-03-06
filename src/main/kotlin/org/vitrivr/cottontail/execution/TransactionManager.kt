package org.vitrivr.cottontail.execution

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.*
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import java.util.*
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.ArrayList

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [Transaction]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class TransactionManager(private val executor: ThreadPoolExecutor) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionManager::class.java)
        private const val TRANSACTION_TABLE_SIZE = 100
        private const val TRANSACTION_HISTORY = 500
    }

    /** The [ExecutorCoroutineDispatcher] used for executing queries. */
    private val dispatcher = this.executor.asCoroutineDispatcher()

    /** Map of [Transaction]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<Transaction>(TRANSACTION_TABLE_SIZE, VERY_FAST_LOAD_FACTOR))

    /** Internal counter to generate [TransactionId]s. */
    private val tidCounter = AtomicLong()

    /** List of ongoing or past transactions (limited to [TRANSACTION_HISTORY] entries). */
    val transactionHistory = Collections.synchronizedList(ArrayList<Transaction>(TRANSACTION_HISTORY))

    /** The [LockManager] instance used by this [TransactionManager]. */
    val lockManager = LockManager()

    /**
     * Returns the [Transaction] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [Transaction] for.
     * @return [Transaction] or null
     */
    operator fun get(txId: TransactionId): Transaction? = this.transactions[txId]

    /**
     * A concrete [Transaction] used for executing a query.
     *
     * A [Transaction] can be of different [TransactionType]s. Their execution semantic may slightly
     * differ depending on that type.
     *
     * @author Ralph Gasser
     * @version 1.2.0
     */
    inner class Transaction(override val type: TransactionType) : LockHolder(this@TransactionManager.tidCounter.getAndIncrement()), TransactionContext {

        /** The [TransactionStatus] of this [Transaction]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.READY
            private set

        /** Map of all [Tx] that have been created as part of this [Transaction]. */
        private val txns: MutableMap<DBO, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** Number of [Tx] held by this [Transaction]. */
        val numberOfTxs: Int = this.txns.size

        /** Timestamp of when this [Transaction] was created. */
        val created = System.currentTimeMillis()

        /** Timestamp of when this [Transaction] was either COMMITTED or ABORTED. */
        var ended: Long? = null
            private set

        init {
            /** Add this to transaction history and transaction table. */
            if (this@TransactionManager.transactionHistory.size >= TRANSACTION_HISTORY) {
                (0 until 50).forEach { this@TransactionManager.transactionHistory.removeAt(it) }
            }
            this@TransactionManager.transactions[this.txId] = this
            this@TransactionManager.transactionHistory.add(this)
        }

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [Transaction] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        override fun getTx(dbo: DBO): Tx {
            return this.txns.computeIfAbsent(dbo) {
                dbo.newTx(this)
            }
        }


        /**
         * Acquires a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
         * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to request the lock for.
         * @param mode The desired [LockMode]
         */
        override fun requestLock(dbo: DBO, mode: LockMode) {
            this@TransactionManager.lockManager.lock(this, dbo, mode)
        }

        /**
         * Releases a [Lock] on a [DBO]. This call is delegated to the [LockManager] and really just
         * a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to release the lock for.
         */
        override fun releaseLock(dbo: DBO) {
            this@TransactionManager.lockManager.unlock(this, dbo)
        }

        /**
         * Returns the [LockMode] this [Transaction] has on the given [DBO].
         *
         * @param dbo [DBO] The [DBO] to query the [LockMode] for.
         * @return [LockMode]
         */
        override fun lockOn(dbo: DBO): LockMode {
            return this@TransactionManager.lockManager.lockOn(this, dbo)
        }

        /**
         * Signals a [DataChangeEvent] to this [Transaction].
         *
         * Implementing methods must process these [DataChangeEvent]s quickly, since they are usually
         * triggered during an ongoing transaction.
         *
         * @param event The [DataChangeEvent] that has been reported.
         */
        override fun signalEvent(event: DataChangeEvent) {
            /* ToDo: Do something with the events. */
        }

        /**
         * Schedules an [Operator.SinkOperator] for execution in this [Transaction] and blocks,
         * until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        @Synchronized
        fun execute(operator: Operator.SinkOperator) {
            runBlocking(this@TransactionManager.dispatcher) {
                try {
                    this@Transaction.state = TransactionStatus.RUNNING
                    operator.toFlow(this@Transaction).collect()
                    this@Transaction.state = TransactionStatus.READY
                } catch (e: DeadlockException) {
                    this@Transaction.state = TransactionStatus.ERROR
                    throw TransactionException.DeadlockException(this@Transaction.txId, e)
                } catch (e: ExecutionException.OperatorExecutionException) {
                    this@Transaction.state = TransactionStatus.ERROR
                    throw e
                } catch (e: DatabaseException) {
                    this@Transaction.state = TransactionStatus.ERROR
                    throw e
                } catch (e: Throwable) {
                    this@Transaction.state = TransactionStatus.ERROR
                    throw ExecutionException("Unhandled exception during execution of transaction ${this@Transaction.txId}: ${e.message}")
                }
            }
        }

        /**
         * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
         */
        @Synchronized
        fun commit() {
            check(this.state === TransactionStatus.READY) { "Cannot commit transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            try {
                this.txns.values.forEach { txn ->
                    txn.commit()
                    txn.close()
                }
            } catch (e: Throwable) {
                LOGGER.error("An error occurred while committing transaction ${this.txId}. This is probably serious!", e)
            } finally {
                this.txns.clear()
                this.ended = System.currentTimeMillis()
                this.state = TransactionStatus.ROLLBACK
                this@TransactionManager.transactions.remove(this.txId)
                Unit
            }
        }

        /**
         * Rolls back this [Transaction] thus reverting all operations executed so far.
         */
        @Synchronized
        fun rollback() {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.ERROR) { "Cannot rollback transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            try {
                this.txns.values.forEach { txn ->
                    txn.rollback()
                    txn.close()
                }
            } catch (e: Throwable) {
                LOGGER.error("An error occurred while rolling back transaction ${this.txId}. This is probably serious!", e)
            } finally {
                this.txns.clear()
                this.ended = System.currentTimeMillis()
                this.state = TransactionStatus.ROLLBACK
                this@TransactionManager.transactions.remove(this.txId)
                Unit
            }
        }
    }
}