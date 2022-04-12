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
     * Processes incoming [IndexEvent] and determines which [Index] require re-building.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun didCommit(txId: TransactionId, events: List<Event>) {
        val set = ObjectOpenHashSet<Name.IndexName>()
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
            this.manager.executionManager.serviceWorkerPool.schedule(Task(index), 100L, TimeUnit.MILLISECONDS)
        }
    }

    /**
     * The [AutoRebuilderService] has nothing to do after a transaction has been aborted.
     */
    override fun didAbort(txId: TransactionId, events: List<Event>) { /* No op. */ }


    /**
     * The actual [Runnable] that rebuilds an [Index].
     */
    inner class Task(private val index: Name.IndexName): Runnable {
        override fun run() {
            val transaction = this@AutoRebuilderService.manager.TransactionImpl(TransactionType.SYSTEM)
            try {
                LOGGER.info("Starting index auto-rebuilding for $index.")
                val duration = measureTimeMillis {
                    val catalogueTx = transaction.getTx(this@AutoRebuilderService.catalogue) as CatalogueTx
                    val schema = catalogueTx.schemaForName(index.schema())
                    val schemaTx = transaction.getTx(schema) as SchemaTx
                    val entity = schemaTx.entityForName(index.entity())
                    val entityTx = transaction.getTx(entity) as EntityTx
                    val index = entityTx.indexForName(index)
                    val indexTx = transaction.getTx(index) as IndexTx
                    if (indexTx.state == IndexState.DIRTY) {
                        indexTx.rebuild()
                    }
                    transaction.commit()
                }
                LOGGER.info("Index auto-rebuilding for $index completed (took ${duration}ms).")
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.IndexDoesNotExistException -> LOGGER.error("Index auto-rebuilding for $index failed because DBO no longer exists.")
                    else -> LOGGER.error("Index auto-rebuilding for $index failed due to exception: ${e.message}.")
                }
                transaction.rollback()
            }
        }
    }
}