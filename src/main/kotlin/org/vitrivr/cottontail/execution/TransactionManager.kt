package org.vitrivr.cottontail.execution

import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.*
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [Transaction]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class TransactionManager(config: ExecutionConfig) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionManager::class.java)
    }

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(
        config.coreThreads,
        config.maxThreads,
        config.keepAliveMs,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(config.queueSize)
    )

    /** The [LockManager] instance used by this [TransactionManager]. */
    private val lockManager = LockManager()

    /** The [ExecutorCoroutineDispatcher] used for executing queries. */
    private val dispatcher = this.executor.asCoroutineDispatcher()

    /** Map of [Transaction]s that are currently PENDING or RUNNING. */
    private val transactions = Long2ObjectLinkedOpenHashMap<Transaction>()

    /** Internal counter to generate [TransactionId]s. */
    private val tidCounter = AtomicLong()

    /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
    val availableThreads
        get() = this@TransactionManager.executor.maximumPoolSize - this@TransactionManager.executor.activeCount

    /**
     * Returns the [Transaction] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [Transaction] for.
     * @return [Transaction] or null
     */
    operator fun get(txId: TransactionId): Transaction? = this.transactions[txId]

    /**
     *
     */
    fun transactions() = this.transactions.values.toList()

    /**
     * A concrete [Transaction] used for executing a query.
     *
     * A [Transaction] can be of different [TransactionType]s. Their execution semantic may slightly
     * differ depending on that type.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class Transaction(val type: TransactionType) : LockHolder(this@TransactionManager.tidCounter.getAndIncrement()), TransactionContext {

        /** The [TransactionStatus] of this [Transaction]. */
        @Volatile
        var state: TransactionStatus = TransactionStatus.READY
            private set

        /** Map of all [Tx] that have been created as part of this [Transaction]. */
        private val txns: MutableMap<DBO, Tx> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

        /** A [ReentrantLock] that mediates access to primitives that govern this [Transaction]. */
        private val lock = ReentrantLock()

        /** Number of [Tx] held by this [Transaction]. */
        val numberOfTxs: Int = this.txns.size

        /** Timestamp of when this [Transaction] was created. */
        val created = System.currentTimeMillis()

        /** Timestamp of when this [Transaction] was either COMMITTED or ABORTED. */
        var ended: Long? = null
            private set

        /** Reference to the [TransactionManager]s [ExecutorCoroutineDispatcher].*/
        override val dispatcher
            get() = this@TransactionManager.dispatcher

        /** Add to list of running contexts. */
        init {
            this@TransactionManager.transactions[this.txId] = this
        }

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [Transaction] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        override fun getTx(dbo: DBO): Tx = this.txns.computeIfAbsent(dbo) {
            dbo.newTx(this)
        }

        /**
         * Acquires a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
         * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to request the lock for.
         * @param mode The desired [LockMode]
         */
        override fun requestLock(dbo: DBO, mode: LockMode) = this@TransactionManager.lockManager.lock(this, dbo, mode)

        /**
         * Releases a [Lock] on a [DBO]. This call is delegated to the [LockManager] and really just
         * a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to release the lock for.
         */
        override fun releaseLock(dbo: DBO) = this@TransactionManager.lockManager.unlock(this, dbo)

        /**
         * Returns the [LockMode] this [Transaction] has on the given [DBO].
         *
         * @param dbo [DBO] The [DBO] to query the [LockMode] for.
         * @return [LockMode]
         */
        override fun lockOn(dbo: DBO) = this@TransactionManager.lockManager.lockOn(this, dbo)

        /**
         * Schedules an [Operator.SinkOperator] for execution in this [Transaction] and blocks,
         * until execution has completed.
         *
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        fun execute(operator: Operator.SinkOperator) {
            check(this.state === TransactionStatus.READY) { "Could not schedule operator for transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this.lock.withLock {
                runBlocking(this.dispatcher) {
                    try {
                        this@Transaction.state = TransactionStatus.RUNNING
                        operator.toFlow(this@Transaction).collect()
                        this@Transaction.state = TransactionStatus.READY
                    } catch (e: DeadlockException) {
                        LOGGER.debug("Deadlock encountered during execution of transaction ${this@Transaction.txId}.", e)
                        this@Transaction.state = TransactionStatus.ERROR
                        throw e
                    } catch (e: OperatorExecutionException) {
                        LOGGER.debug("Unhandled exception during operator execution in transaction ${this@Transaction.txId}.", e)
                        this@Transaction.state = TransactionStatus.ERROR
                        throw e
                    } catch (e: DatabaseException) {
                        LOGGER.warn("Unhandled database exception during execution of transaction ${this@Transaction.txId}.", e)
                        this@Transaction.state = TransactionStatus.ERROR
                        throw ExecutionException("Unhandled exception during execution of transaction ${this@Transaction.txId}: ${e.message}")
                    } catch (e: Throwable) {
                        LOGGER.error("Unhandled exception during query execution of transaction ${this@Transaction.txId}.", e)
                        this@Transaction.state = TransactionStatus.ERROR
                        throw ExecutionException("Unhandled exception during execution of transaction ${this@Transaction.txId}: ${e.message}.")
                    }
                }
            }
        }

        /**
         * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
         */
        fun commit() = this.lock.withLock {
            check(this.state === TransactionStatus.READY) { "Cannot commit transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this@Transaction.txns.values.forEach { txn ->
                txn.commit()
                txn.close()
            }
            this.txns.clear()
            this.ended = System.currentTimeMillis()
            this.state = TransactionStatus.COMMIT
        }

        /**
         * Rolls back this [Transaction] thus reverting all operations executed so far.
         */
        fun rollback() = this.lock.withLock {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.ERROR) { "Cannot rollback transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this@Transaction.txns.values.forEach { txn ->
                txn.rollback()
                txn.close()
            }
            this.txns.clear()
            this.ended = System.currentTimeMillis()
            this.state = TransactionStatus.ROLLBACK
        }
    }
}