package org.vitrivr.cottontail.dbms.execution.services

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.events.ColumnEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * A [TransactionObserver] that listens for [ColumnEvent.Stale]s and triggers analysis of [Column]es whose statistic has become stale.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class AutoAnalyzerService(private val catalogue: Catalogue, private val manager: TransactionManager): TransactionObserver {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AutoAnalyzerService::class.java)
    }

    /** Internal counter to keep track of then umber of spawned tasks. */
    private val counter = AtomicLong(0L)

    /**
     * The [AutoAnalyzerService] is only interested int [ColumnEvent.Stale].
     *
     * @param event The [Event] to check.
     * @return True if [Event] is a [ColumnEvent.Stale]
     */
    override fun isRelevant(event: Event): Boolean
        = event is ColumnEvent.Stale

    /**
     * Processes incoming [IndexEvent] and determines which [Index] require re-building.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun onCommit(txId: TransactionId, events: List<Event>) {
        val set = ObjectOpenHashSet<Name.ColumnName>()
        for (event in events) {
            set.add((event as ColumnEvent.Stale).column)
        }

        /* Schedule task for each index that needs rebuilding. */
        for (column in set) {
            this.manager.executionManager.serviceWorkerPool.schedule(Task(column), 100L, TimeUnit.MILLISECONDS)
        }
    }

    /**
     * The [AutoAnalyzerService] cannot do anything if there is a delivery failure.
     */
    override fun onDeliveryFailure(txId: TransactionId) {
        /* No op. */
    }

    /**
     * The actual [Runnable] that executes [Column] analysis.
     */
    inner class Task(private val column: Name.ColumnName): Runnable {
        override fun run() {
            val transaction = this@AutoAnalyzerService.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
            val context = DefaultQueryContext("auto-analysis-${this@AutoAnalyzerService.counter.incrementAndGet()}", this@AutoAnalyzerService.catalogue, transaction)
            try {
                LOGGER.info("Starting auto-analysis of column $column...")
                val catalogueTx = this@AutoAnalyzerService.catalogue.newTx(context)
                val schema = catalogueTx.schemaForName(this.column.schema() ?: return)
                val schemaTx = schema.newTx(context)
                val entity = schemaTx.entityForName(this.column.entity() ?: return)
                val entityTx = entity.newTx(context)
                val column = entityTx.columnForName(this.column)
                val columnTx = column.newTx(context)
                if (!columnTx.statistics().fresh) {
                    columnTx.analyse()
                }
                transaction.commit()
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.ColumnDoesNotExistException -> LOGGER.error("Auto-analysis of column $column failed because DBO no longer exists.")
                    else -> LOGGER.error("Auto-analysis of column $column failed due to exception: ${e.message}.")
                }
                transaction.rollback()
            }
        }
    }
}