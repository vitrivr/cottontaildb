package org.vitrivr.cottontail.dbms.execution.services

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

/**
 * A [TransactionObserver] that listens for [IndexEvent]s and triggers rebuilding of [Index]es that become [IndexState.STALE].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AutoRebuilderService(val catalogue: Catalogue, val manager: TransactionManager): TransactionObserver {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AutoRebuilderService::class.java)
    }

    /**
     * The [AutoRebuilderService] is only interested int [IndexEvent]s.
     *
     * @param event The [Event] to check.
     * @return True if [Event] is an [IndexEvent]
     */
    override fun isRelevant(event: Event): Boolean
        = event is IndexEvent

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
            this.manager.executionManager.serviceWorkerPool.schedule(Task(index, type), 100L, TimeUnit.MILLISECONDS)
        }
    }

    /**
     * The [AutoRebuilderService] cannot do anything if there is a delivery failure.
     */
    override fun onDeliveryFailure(txId: TransactionId) {
        /* No op. */
    }


    /**
     * The actual [Runnable] that rebuilds an [Index].
     */
    inner class Task(private val index: Name.IndexName, private val type: IndexType): Runnable {
        override fun run() {
            val duration = measureTimeMillis {
                if (this.type.descriptor.supportsAsyncRebuild && this.type.descriptor.supportsIncrementalUpdate) {
                    LOGGER.info("Starting asynchronous index auto-rebuilding for $index.")
                    this.performAsynchronousRebuild()
                } else {
                    LOGGER.info("Starting index auto-rebuilding for $index.")
                    this.performSynchronousRebuild()
                }
            }
            LOGGER.info("Index auto-rebuilding for $index completed (took ${duration}ms).")


        }

        /**
         * Synchronous [Index] rebuilding.
         */
        private fun performSynchronousRebuild() {
            val transaction = this@AutoRebuilderService.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
            try {
                val catalogueTx = transaction.getTx(this@AutoRebuilderService.catalogue) as CatalogueTx
                val schema = catalogueTx.schemaForName(this.index.schema())
                val schemaTx = transaction.getTx(schema) as SchemaTx
                val entity = schemaTx.entityForName(this.index.entity())
                val entityTx = transaction.getTx(entity) as EntityTx
                val index = entityTx.indexForName(this.index)
                val indexTx = transaction.getTx(index) as IndexTx
                if (indexTx.state != IndexState.CLEAN) {
                    indexTx.rebuild()
                }
                transaction.commit()
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.warn("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding for $index failed due to exception: ${e.message}.")
                }
                transaction.rollback()
            }
        }

        /**
         * Performs asynchronous index rebuilding in two steps (SCAN -> MERGE).
         */
        private fun performAsynchronousRebuild() {
            /* Step 1: Scan index (read-only). */
            val transaction1 = this@AutoRebuilderService.manager.TransactionImpl(TransactionType.SYSTEM_READONLY)
            val rebuilder = try {
                val catalogueTx = transaction1.getTx(this@AutoRebuilderService.catalogue) as CatalogueTx
                val schema = catalogueTx.schemaForName(this.index.schema())
                val schemaTx = transaction1.getTx(schema) as SchemaTx
                val entity = schemaTx.entityForName(this.index.entity())
                val entityTx = transaction1.getTx(entity) as EntityTx
                val index = entityTx.indexForName(this.index)
                val indexTx = transaction1.getTx(index) as IndexTx
                if (indexTx.state != IndexState.CLEAN) {
                    indexTx.asyncRebuild()
                } else {
                    return
                }
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.warn("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding (scan) for $index failed due to exception: ${e.message}.")
                }
                return
            } finally {
                transaction1.rollback()
            }

            /* Step 2: Merge index (write). */
            val transaction2 = this@AutoRebuilderService.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
            try {
                rebuilder.merge(transaction2)
                transaction2.commit()
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.warn("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding (merge) for $index failed due to exception: ${e.message}.")
                }
                transaction2.rollback()
            }
        }
    }
}