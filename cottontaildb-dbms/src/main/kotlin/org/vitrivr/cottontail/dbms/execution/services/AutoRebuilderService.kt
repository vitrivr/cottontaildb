package org.vitrivr.cottontail.dbms.execution.services

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilderState
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.server.Instance
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * A [TransactionObserver] that listens for [IndexEvent]s and triggers rebuilding of [Index]es that become [IndexState.STALE].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class AutoRebuilderService(private val instance: Instance): TransactionObserver {

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
        val set = ObjectOpenHashSet<Index>()
        for (event in events) {
            when (event) {
                is IndexEvent.State -> if (event.state == IndexState.STALE) {
                    set.add(event.index)
                } else {
                    set.remove(event.index)
                }
                is IndexEvent.Created -> set.add(event.index)
                is IndexEvent.Dropped -> set.remove(event.index)
                else -> { /* No op. */ }
            }
        }

        /* Schedule task for each index that needs rebuilding. */
        for (index in set) {
            this.schedule(index)
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
     * @param index The [Index] to rebuild.
     */
    fun schedule(index: Index) {
        this.instance.executor.serviceWorkerPool.schedule(Task(index), 500L, TimeUnit.MILLISECONDS)
    }

    /**
     * The actual [Runnable] that rebuilds an [Index].
     */
    inner class Task(index: Index): Runnable {

        /** A weak reference to the [Index] that should be rebuilt; if index is removed while this tasks is waiting for execution, rebuild can be skipped. */
        private val index = WeakReference(index)

        override fun run() {
            val index = this.index.get() ?: return
            val start = System.currentTimeMillis()
            val success = if (index.type.descriptor.supportsAsyncRebuild && index.type.descriptor.supportsIncrementalUpdate) {
                LOGGER.info("Starting asynchronous index auto-rebuilding for $index.")
                this.performConcurrentRebuild(index)
            } else {
                LOGGER.info("Starting index auto-rebuilding for $index.")
                this.performRebuild(index)
            }
            val end = System.currentTimeMillis()
            if (success) {
                LOGGER.info("Index auto-rebuilding for $index completed (took ${end-start}ms).")
            } else {
                this.tryScheduleRetry(index)
            }
        }

        /**
         * Tries to schedule another task if the task before has failed. This is repeated up to [MAX_INDEX_REBUILDING_RETRY] times.
         *
         * @param index The [Index] to rebuild.
         */
        private fun tryScheduleRetry(index: Index) {
            val failures = this@AutoRebuilderService.failures.compute(index.name) { _, v -> (v ?: 0) + 1 }
            if (failures!! <= MAX_INDEX_REBUILDING_RETRY) {
                LOGGER.warn("Index auto-rebuilding for $index failed $failures time(s). Re-scheduling the task...")
                this@AutoRebuilderService.instance.executor.serviceWorkerPool.schedule(Task(index), 5000L, TimeUnit.MILLISECONDS)
            } else {
                LOGGER.error("Index auto-rebuilding for $index failed $failures time(s). Aborting...")
                this@AutoRebuilderService.failures.remove(index.name)
            }
        }

        /**
         * Synchronous [Index] rebuilding.
         *
         * @param index The [Index] to rebuild.
         * @return True on success, false otherwise.
         */
        private fun performRebuild(index: Index): Boolean {
            val transaction = this@AutoRebuilderService.instance.transactions.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
            val context = DefaultQueryContext("auto-rebuild-${this@AutoRebuilderService.counter.incrementAndGet()}", this@AutoRebuilderService.instance, transaction)
            try {
                val schema = context.catalogueTx.schemaForName(index.name.schema())
                val schemaTx = schema.newTx(context.catalogueTx)
                val entity = schemaTx.entityForName(index.name.entity())
                val entityTx = entity.createOrResumeTx(schemaTx)
                val indexTx = index.newTx(entityTx)
                val ret = index.newRebuilder(context).rebuild(indexTx)
                if (ret) {
                    transaction.commit()
                } else {
                    transaction.abort()
                }
                return ret
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.warn("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding for $index failed due to exception: ${e.message}.")
                }
                transaction.abort()
                return false
            }
        }

        /**
         * Performs asynchronous index rebuilding in two steps (SCAN -> MERGE).
         *
         * @param index The [Index] to rebuild.
         * @return True on success, false otherwise.
         */
        private fun performConcurrentRebuild(index: Index): Boolean {
            /* Step 1a: Scan index (read-only). */
            val rebuilder = index.newAsyncRebuilder(this@AutoRebuilderService.instance)

            try {
                /* Step 1: Start BUILD process and perform sanity check to prevent obtaining an exclusive transaction unnecessarily. */
                rebuilder.build()
                if (rebuilder.state != IndexRebuilderState.REBUILT) {
                    return false
                }

                /* Step 2: Start REPLACE process (write). */
                rebuilder.replace()
                return rebuilder.state == IndexRebuilderState.FINISHED
            } catch (e: Throwable) {
                LOGGER.error("Index auto-rebuilding (MERGE) for $index failed due to exception: ${e.message}.")
                return false
            } finally {
                this@AutoRebuilderService.instance.transactions.deregister(rebuilder)
                rebuilder.close()
            }
        }
    }
}