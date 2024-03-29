package org.vitrivr.cottontail.dbms.execution.services

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * A [TransactionObserver] that listens for [IndexEvent]s and triggers rebuilding of [Index]es that become [IndexState.STALE].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class AutoRebuilderService(private val catalogue: Catalogue): TransactionObserver {

    companion object {
        private const val MAX_INDEX_REBUILDING_RETRY = 3
        private val LOGGER = LoggerFactory.getLogger(AutoRebuilderService::class.java)
    }

    /** Tracks failures for index rebuilding. */
    private val failures = ConcurrentHashMap<Name.IndexName, Int>(10)

    /** Internal counter to keep track of then number of spawned index rebuilders. */
    private val counter = AtomicLong(0L)

    /**
     * The [AutoRebuilderService] is only interested int [IndexEvent]s.
     *
     * @param event The [Event] to check.
     * @return True if [Event] is an [IndexEvent]
     */
    override fun isRelevant(event: Event): Boolean = event is IndexEvent

    /**
     * Processes incoming [IndexEvent] and determines which [Index] require re-building.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun onCommit(txId: TransactionId, events: List<Event>) {
        val set = ObjectOpenHashSet<Pair<Name.IndexName, IndexType>>()
        for (event in events) {
            when (event) {
                is IndexEvent.State -> if (event.state == IndexState.STALE) {
                    set.add(event.index to event.type)
                } else {
                    set.remove(event.index to event.type)
                }
                is IndexEvent.Created -> set.add(event.index to event.type)
                is IndexEvent.Dropped -> set.remove(event.index to event.type)
                else -> { /* No op. */ }
            }
        }

        /* Schedule task for each index that needs rebuilding. */
        for ((index, type) in set) {
            this.schedule(index, type)
        }
    }

    /**
     * The [AutoRebuilderService] cannot do anything if there is a delivery failure.
     */
    override fun onDeliveryFailure(txId: TransactionId) {
        /* No op. */
    }

    /**
     * Schedules a new [Task] for rebuilding the specified index.
     *
     * @param index The [Name.IndexName] of the [Index] to rebuild.
     * @param type The [IndexType] of the [Index] to rebuild.
     */
    fun schedule(index: Name.IndexName, type: IndexType) {
        this.catalogue.transactionManager.executionManager.serviceWorkerPool.schedule(Task(index, type), 500L, TimeUnit.MILLISECONDS)
    }

    /**
     * The actual [Runnable] that rebuilds an [Index].
     */
    inner class Task(private val index: Name.IndexName, private val type: IndexType): Runnable {
        override fun run() {
            val start = System.currentTimeMillis()
            val success = if (this.type.descriptor.supportsAsyncRebuild && this.type.descriptor.supportsIncrementalUpdate) {
                LOGGER.info("Starting asynchronous index auto-rebuilding for $index.")
                this.performConcurrentRebuild()
            } else {
                LOGGER.info("Starting index auto-rebuilding for $index.")
                this.performRebuild()
            }
            val end = System.currentTimeMillis()
            if (success) {
                LOGGER.info("Index auto-rebuilding for $index completed (took ${end-start}ms).")
            } else {
                this.tryScheduleRetry()
            }
        }

        /**
         * Tries to schedule a another task if the task before has failed. This is repeated up to [MAX_INDEX_REBUILDING_RETRY] times.
         */
        private fun tryScheduleRetry() {
            val failures = this@AutoRebuilderService.failures.compute(this.index) { _, v -> (v ?: 0) + 1 }
            if (failures!! <= MAX_INDEX_REBUILDING_RETRY) {
                LOGGER.warn("Index auto-rebuilding for $index failed $failures time(s). Re-scheduling the task...")
                this@AutoRebuilderService.catalogue.transactionManager.executionManager.serviceWorkerPool.schedule(Task(this.index, this.type), 5000L, TimeUnit.MILLISECONDS)
            } else {
                LOGGER.error("Index auto-rebuilding for $index failed $failures time(s). Aborting...")
                this@AutoRebuilderService.failures.remove(this.index)
            }
        }

        /**
         * Synchronous [Index] rebuilding.
         *
         * @return True on success, false otherwise.
         */
        private fun performRebuild(): Boolean {
            val transaction = this@AutoRebuilderService.catalogue.transactionManager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
            val context = DefaultQueryContext("auto-rebuild-${this@AutoRebuilderService.counter.incrementAndGet()}", this@AutoRebuilderService.catalogue, transaction)
            try {
                val catalogueTx = this@AutoRebuilderService.catalogue.newTx(context)
                val schema = catalogueTx.schemaForName(this.index.schema())
                val schemaTx = schema.newTx(context)
                val entity = schemaTx.entityForName(this.index.entity())
                val entityTx = entity.newTx(context)
                val index = entityTx.indexForName(this.index)
                val ret = index.newRebuilder(context).rebuild()
                if (ret) {
                    transaction.commit()
                } else {
                    transaction.rollback()
                }
                return ret
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.warn("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding for $index failed due to exception: ${e.message}.")
                }
                transaction.rollback()
                return false
            }
        }

        /**
         * Performs asynchronous index rebuilding in two steps (SCAN -> MERGE).
         *
         * @return True on success, false otherwise.
         */
        private fun performConcurrentRebuild(): Boolean {
            /* Step 1a: Scan index (read-only). */
            val transaction = this@AutoRebuilderService.catalogue.transactionManager.startTransaction(TransactionType.SYSTEM_READONLY)
            val context = DefaultQueryContext("auto-rebuild-prepare", this@AutoRebuilderService.catalogue, transaction)
            val rebuilder = try {
                val catalogueTx = this@AutoRebuilderService.catalogue.newTx(context)
                val schema = catalogueTx.schemaForName(this.index.schema())
                val schemaTx = schema.newTx(context)
                val entity = schemaTx.entityForName(this.index.entity())
                val entityTx = entity.newTx(context)
                val index = entityTx.indexForName(this.index)
                val ret = index.newAsyncRebuilder(context)
                transaction.commit()
                ret
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.warn("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding (SCAN) for $index failed due to exception: ${e.message}.")
                }
                transaction.rollback()
                return false
            }

            try {
                /* Step 1: Start BUILD process and perform sanity check to prevent obtaining an exclusive transaction unnecessarily. */
                rebuilder.build()
                if (rebuilder.state != IndexRebuilderState.REBUILT) {
                    return false
                }

                /* Step 2: Start REPLACE process (write). */
                rebuilder.replace()
                if (rebuilder.state != IndexRebuilderState.FINISHED) {
                    return false
                }

                return true
            } catch (e: Throwable) {
                LOGGER.error("Index auto-rebuilding (MERGE) for $index failed due to exception: ${e.message}.")
                return false
            } finally {
                this@AutoRebuilderService.catalogue.transactionManager.deregister(rebuilder)
                rebuilder.close()
            }
        }
    }
}