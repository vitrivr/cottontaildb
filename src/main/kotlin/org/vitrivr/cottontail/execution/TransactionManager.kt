package org.vitrivr.cottontail.execution

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.*
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.TransactionId
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
    private val transactions = Long2ObjectOpenHashMap<Transaction>()

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
     * A concrete [Transaction] used for executing a query.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class Transaction : LockHolder(this@TransactionManager.tidCounter.getAndIncrement()), TransactionContext {

        /** The [TransactionStatus] of this [Transaction]. */
        @Volatile
        var state: TransactionStatus = TransactionStatus.OPEN
            private set

        /** Map of all [Entity.Tx] that have been created as part of this [Transaction]. */
        private val txns: MutableMap<DBO, Tx> = mutableMapOf()

        /** A [ReentrantLock] that mediates access to primitives that govern this [Transaction]. */
        private val lock = ReentrantLock()

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
        override fun getTx(dbo: DBO): Tx {
            this.txns.putIfAbsent(dbo, dbo.newTx(this))
            return this.txns[dbo]!!
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
         * Schedules an [Operator.SinkOperator] for execution in this [Transaction].
         */
        fun execute(operator: Operator.SinkOperator) = this.lock.withLock {
            check(this.state === TransactionStatus.OPEN) { "Could not schedule operator for transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            runBlocking(this.dispatcher) {
                try {
                    operator.toFlow(this@Transaction).collect()
                } catch (e: DeadlockException) {
                    LOGGER.debug("Deadlock encountered during execution of transaction ${this@Transaction.txId}.", e)
                    this@Transaction.state = TransactionStatus.ERROR
                    throw e
                } catch (e: OperatorExecutionException) {
                    LOGGER.debug("Unhandled exception during operator execution in transaction ${this@Transaction.txId}.", e)
                    this@Transaction.state = TransactionStatus.ERROR
                    throw e
                } catch (e: Throwable) {
                    LOGGER.debug("Unhandled exception during query execution of transaction ${this@Transaction.txId}.", e)
                    this@Transaction.state = TransactionStatus.ERROR
                    throw ExecutionException("Unhandled exception during execution of transaction ${this@Transaction.txId}.")
                }
            }
        }

        /**
         * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
         */
        fun commit() = this.lock.withLock {
            check(this.state === TransactionStatus.OPEN) { "Cannot commit transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this@Transaction.txns.values.forEach { txn ->
                txn.commit()
                txn.close()
            }
            this.state = TransactionStatus.COMMIT
        }

        /**
         * Rolls back this [Transaction] thus reverting all operations executed so far.
         */
        fun rollback() = this.lock.withLock {
            check(this.state === TransactionStatus.OPEN || this.state === TransactionStatus.ERROR) { "Cannot rollback transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this@Transaction.txns.values.forEach { txn ->
                txn.rollback()
                txn.close()
            }
            this.state = TransactionStatus.ROLLBACK
        }
    }
}