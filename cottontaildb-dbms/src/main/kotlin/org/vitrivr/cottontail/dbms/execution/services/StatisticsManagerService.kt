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
import java.time.Instant

/**
 * A [TransactionObserver] that keeps track of different Columns and triggers an analysis for the columns that have reached a specific threshold.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
class StatisticsManagerService(private val catalogue: Catalogue, private val manager: TransactionManager): TransactionObserver {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(StatisticsManagerService::class.java)
    }

    /** Internal counter to keep track of then umber of spawned tasks. */
    private val counter = AtomicLong(0L)

    /**
     * The [StatisticsManagerService] is interested in all [ColumnEvent]'s.
     *
     * @param event The [Event] to check.
     * @return True
     */
    override fun isRelevant(event: Event): Boolean {
        return event is ColumnEvent
    }

    /**
     * Processes incoming [IndexEvent] and determines which [Index] require re-building.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun onCommit(txId: TransactionId, events: List<Event>) {
        // DONE("Keep track of the transactions/event on a column basis")
        for (event in events) {
            this.increaseChangeCount((event as ColumnEvent).column)
        }
        // And

        // DONE("If the threshold for a specific column was reached, schedule a task for that")
        // --> Direct implemented in increaseChangeCount method
        /*
        val set = ObjectOpenHashSet<Name.ColumnName>() // a set to which we add all columns that need to be updated
        for (column in set) {
            this.schedule(column) // schedule a task for all of these
        }
        */
    }

    /**
     * The [StatisticsManagerService] does not care about a delivery failure at the moment.
     */
    override fun onDeliveryFailure(txId: TransactionId) {
        /* No op. */
    }

    /**
     * T
     */

    /**
     * Schedules a new [Task] for analysing the specified column.
     *
     * @param column The [Name.ColumnName] of the [Column] to analyse.
     */
    private fun schedule(column: Name.ColumnName) {
        var task: Runnable = Task(column)
        this.manager.executionManager.serviceWorkerPool.schedule(task, 100L, TimeUnit.MILLISECONDS)
    }

    /**
     * The actual [Runnable] that executes [Column] analysis.
     */
    inner class Task(private val column: Name.ColumnName): Runnable {
        override fun run() {
            TODO("To be implemented. Maybe done in the statCollector?")
        }
    }

    /**
     * changes is a temporary data struct that tracks the number of changes (and when the last change occurred) in memory
     * Basically in the form of ColumnName: <500 Changes, Timestamp>
     */
    private val changes: MutableMap<Name.ColumnName, Pair<Int, Instant>> = mutableMapOf()

    /**
     * threshold is the number of updates that must occur before the task is run
     * TODO Define a default and get via Config file or similar
     */
    private val changesThreshold: Int = 1000

    /**
     * threshold is the seconds that must pass before the task is run
     * TODO Define a default and get via Config file or similar
     */
    private val timestampThreshold: Long = 600 // 600 seconds

    /**
     * This function is called every time a new change was made to a column. Its goal is to count the number of changes that are made in a column and trigger a task to redo the statistics if the threshold is reached
     */
    fun increaseChangeCount(column: Name.ColumnName): Unit {
        val (count, timestamp) = this.changes.getOrDefault(column, Pair(0, Instant.now())) // get current count and timestamp or exchange it with the default Pair specified here

        // Check if this change will trigger the task based on number of changes or time passed
        if (count + 1 >= this.changesThreshold || Instant.now().isAfter(timestamp.plusSeconds(this.timestampThreshold))) {
            LOGGER.info("A new task was schedules to recreate statistics for column " + column.schemaName + "." + column.entityName + "." + column.columnName)
            this.schedule(column) // schedule task for this column
            this.changes[column] = Pair(0, Instant.now()) // Reset count to 0.
        } else {
            LOGGER.info("Change does not trigger a new task")
            this.changes[column] = Pair(count + 1, Instant.now()) // increase count by 1 if no task was scheduled
        }
    }

}