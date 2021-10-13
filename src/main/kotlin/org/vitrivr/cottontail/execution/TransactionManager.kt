package org.vitrivr.cottontail.execution

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.Lock
import org.vitrivr.cottontail.database.locking.LockHolder
import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.TransactionManager.TransactionImpl
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TransactionId
import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [TransactionImpl]s.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class TransactionManager(transactionTableSize: Int, private val transactionHistorySize: Int) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionManager::class.java)
    }

    /** Map of [TransactionImpl]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<TransactionImpl>(transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** Internal counter to generate [TransactionId]s. */
    private val tidCounter = AtomicLong()

    /** The [LockManager] instance used by this [TransactionManager]. */
    val lockManager = LockManager<DBO>()

    /** List of ongoing or past transactions (limited to [transactionHistorySize] entries). */
    val transactionHistory = Collections.synchronizedList(LinkedList<TransactionImpl>())

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
        override var state: TransactionStatus = TransactionStatus.READY
            private set

        /** Map of all [Tx] that have been created as part of this [TransactionImpl]. */
        private val txns: MutableMap<DBO, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

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
        override fun getTx(dbo: DBO): Tx {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.RUNNING) {
                "Cannot obtain Tx for DBO '${dbo.name}' for ${this.txId} because it is in wrong state (s = ${this.state})."
            }
            return this.txns.computeIfAbsent(dbo) { dbo.newTx(this) }
        }

        /**
         * Tries to acquire a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
         * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to request the lock for.
         * @param mode The desired [LockMode]
         */
        override fun requestLock(dbo: DBO, mode: LockMode) {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.RUNNING) {
                "Cannot obtain lock on DBO '${dbo.name}' for ${this.txId} because it is in wrong state (s = ${this.state})."
            }
            this@TransactionManager.lockManager.lock(this, dbo, mode)
        }

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
        override fun execute(operator: Operator): Flow<Record> = operator.toFlow(this).onStart {
            this@TransactionImpl.mutex.lock()
            check(this@TransactionImpl.state === TransactionStatus.READY) { "Cannot start execution of transaction ${this@TransactionImpl.txId} because it is in the wrong state (s = ${this@TransactionImpl.state})." }
            this@TransactionImpl.state = TransactionStatus.RUNNING
        }.onEach {
            if (this.state === TransactionStatus.KILLED) throw CancellationException("Transaction $this.txId was killed by the user.")
        }.onCompletion {
            when (it) {
                null -> {
                    this@TransactionImpl.state = TransactionStatus.READY
                    if (this@TransactionImpl.type === TransactionType.USER_IMPLICIT) {
                        this@TransactionImpl.commitInternal() /* Commit implicit transaction on success. */
                    }
                }
                is CancellationException -> {
                    this@TransactionImpl.state = TransactionStatus.KILLED
                    this@TransactionImpl.rollbackInternal() /* Rollback transaction on cancellation. */
                }
                else -> {
                    this@TransactionImpl.state = TransactionStatus.ERROR
                    this@TransactionImpl.rollbackInternal() /* Rollback transaction on error. */
                }
            }

            /* Unlock mutex. */
            this@TransactionImpl.mutex.unlock()
        }

        /**
         * Commits this [TransactionImpl] thus finalizing and persisting all operations executed so far.
         */
        override fun commit() = runBlocking {
            this@TransactionImpl.mutex.withLock {
                this@TransactionImpl.commitInternal()
            }
        }

        /**
         * Internal commit logic: Commits this [TransactionImpl] thus finalizing and persisting all operations executed so far.
         *
         * When calling this method, a lock on [mutex] should be held!
         */
        private fun commitInternal() {
            check(this@TransactionImpl.state === TransactionStatus.READY) { "Cannot commit transaction ${this@TransactionImpl.txId} because it is in wrong state (s = ${this@TransactionImpl.state})." }
            check(this@TransactionImpl.txns.values.none { it.status == TxStatus.ERROR }) { "Cannot commit transaction ${this@TransactionImpl.txId} because some of the participating Tx are in an error state." }
            this@TransactionImpl.state = TransactionStatus.FINALIZING
            try {
                this@TransactionImpl.txns.values.reversed().forEachIndexed { i, txn ->
                    try {
                        txn.commit()
                    } catch (e: Throwable) {
                        LOGGER.error("An error occurred while committing Tx $i (${txn.dbo.name}) of transaction ${this@TransactionImpl.txId}. This is serious!", e)
                    }
                }
            } finally {
                this@TransactionImpl.finalize(true)
            }
        }


        /**
         * Rolls back this [TransactionImpl] thus reverting all operations executed so far.
         */
        override fun rollback() = runBlocking {
            this@TransactionImpl.mutex.withLock {
                this@TransactionImpl.rollbackInternal()
            }
        }

        /**
         * Internal rollback logic: Rolls back this [TransactionImpl] thus reverting all operations executed so far.
         *
         * When calling this method, a lock on [mutex] should be held!
         */
        private fun rollbackInternal() {
            check(this@TransactionImpl.state === TransactionStatus.READY || this@TransactionImpl.state === TransactionStatus.ERROR || this@TransactionImpl.state === TransactionStatus.KILLED) { "Cannot rollback transaction ${this@TransactionImpl.txId} because it is in wrong state (s = ${this@TransactionImpl.state})." }
            this@TransactionImpl.state = TransactionStatus.FINALIZING
            try {
                this@TransactionImpl.txns.values.reversed().forEachIndexed { i, txn ->
                    try {
                        txn.rollback()
                    } catch (e: Throwable) {
                        LOGGER.error("An error occurred while rolling back Tx $i (${txn.dbo.name}) of transaction ${this@TransactionImpl.txId}. This is serious!", e)
                    }
                }
            } finally {
                this@TransactionImpl.finalize(false)
            }
        }

        /**
         * Tries to kill this [TransactionImpl] interrupting all running queries.
         *
         * A call to this method is a best-effort attempt to stop all ongoing queries. After killing a
         * transaction successfully, all changes are rolled back.
         */
        override fun kill() = runBlocking {
            check(this@TransactionImpl.state === TransactionStatus.READY || this@TransactionImpl.state === TransactionStatus.RUNNING) { "Cannot kill transaction ${this@TransactionImpl.txId} because it is in wrong state (s = ${this@TransactionImpl.state})." }
            this@TransactionImpl.state = TransactionStatus.KILLED
            this@TransactionImpl.rollback()
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