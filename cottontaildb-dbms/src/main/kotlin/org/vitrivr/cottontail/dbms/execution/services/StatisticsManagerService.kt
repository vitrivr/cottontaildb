package org.vitrivr.cottontail.dbms.execution.services

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import jetbrains.exodus.bindings.LongBinding
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.events.ColumnEvent
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.operators.sources.partitionFor
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.statistics.columns.ColumnStatistic
import org.vitrivr.cottontail.dbms.statistics.statCollector.BooleanDataCollector
import org.vitrivr.cottontail.dbms.statistics.statCollector.DataCollector
import org.vitrivr.cottontail.dbms.statistics.values.*
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
        return event is DataEvent
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
            //TODO("track via Entity instead of columns")
            //(event as DataEvent).data.get()// columnsdef
            this.increaseChangeCount((event as DataEvent).entity)
        }

    }

    /**
     * The [StatisticsManagerService] does not care about a delivery failure at the moment.
     */
    override fun onDeliveryFailure(txId: TransactionId) {
        /* No op. */
    }

    /**
     * Schedules a new [Task] for analysing the specified column.
     *
     * @param column The [Name.ColumnName] of the [Column] to analyse.
     */
    private fun schedule(entity: Name.EntityName) {
        var task: Runnable = Task(entity)
        this.manager.executionManager.serviceWorkerPool.schedule(task, 100L, TimeUnit.MILLISECONDS)
    }

    /**
     * The actual [Runnable] that executes [Column] analysis.
     */
    inner class Task(private val entityName: Name.EntityName): Runnable {
        override fun run() {
            val transaction = this@StatisticsManagerService.manager.startTransaction(TransactionType.SYSTEM_READONLY) // Nicht exklusive, nur read only
            val context = DefaultQueryContext("statistics-manager-${this@StatisticsManagerService.counter.incrementAndGet()}", this@StatisticsManagerService.catalogue, transaction)
            try {
                StatisticsManagerService.LOGGER.info("Starting analysis of entity $entityName...")
                val catalogueTx = this@StatisticsManagerService.catalogue.newTx(context)
                val schema = catalogueTx.schemaForName(this.entityName.schema() ?: return)
                val schemaTx = schema.newTx(context)
                val entity = schemaTx.entityForName(this.entityName ?: return)
                val entityTx = entity.newTx(context)

                val columns = entityTx.listColumns().toTypedArray()
                val columnsCollector: Array<DataCollector<*>> = emptyArray()

                // get the collectors for all columns
                for (i in columns.indices) {
                    columnsCollector[i] = getCollector(columns[i])
                }

                // create cursor for all columns of this entity and iterate over all of them
                val entityCursor = entityTx.cursor(columns)
                entityCursor.use { cursor ->
                    while (cursor.moveNext()) {
                        val record = cursor.value()
                        // iterate over columns
                        for (i in columns.indices) {
                            val value = record[i]
                            columnsCollector[i].receive(value)
                        }
                    }
                }
                entityCursor.close()

                // Calculate metrics that have to be calculated after the whole batch
                for (i in columnsCollector.indices) {
                    columnsCollector[i].calculate()
                }

                transaction.commit()
            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.ColumnDoesNotExistException -> StatisticsManagerService.LOGGER.error("Statistics Manager analysis of entity $entityName failed because DBO no longer exists.")
                    else -> StatisticsManagerService.LOGGER.error("Statistics Manager analysis of entity $entityName failed due to exception: ${e.message}.")
                }
                transaction.rollback()
            }
        }
    }

    /**
     * Function that, based on the [ColumnDef]'s [Types] returns the corresponding [DataCollector]
     */
    fun getCollector(def: ColumnDef<*>) : DataCollector<*> {
        val collector = when (def.type) {
            Types.Boolean -> BooleanDataCollector()
            else -> BooleanDataCollector()
            /*Types.Byte -> BooleanDataCollector()
            Types.Short -> BooleanDataCollector()
            Types.Date -> BooleanDataCollector()
            Types.Double -> BooleanDataCollector()
            Types.Float -> BooleanDataCollector()
            Types.Int -> BooleanDataCollector()
            Types.Long -> BooleanDataCollector()
            Types.String -> BooleanDataCollector()
            Types.ByteString -> BooleanDataCollector()
            Types.Complex32 -> BooleanDataCollector()
            Types.Complex64 -> BooleanDataCollector()
            is Types.BooleanVector -> BooleanDataCollector(def.type.logicalSize)
            is Types.DoubleVector -> BooleanDataCollector(def.type.logicalSize)
            is Types.FloatVector -> BooleanDataCollector(def.type.logicalSize)
            is Types.IntVector -> BooleanDataCollector(def.type.logicalSize)
            is Types.LongVector -> BooleanDataCollector(def.type.logicalSize)
            is Types.Complex32Vector -> BooleanDataCollector(def.type.logicalSize)
            is Types.Complex64Vector -> BooleanDataCollector(def.type.logicalSize)*/
        }
        return collector
    }

    /**
     * changes is a temporary data struct that tracks the number of changes (and when the last change occurred) in memory
     * Basically in the form of EntityName: <500 Changes, Timestamp>
     */
    private val changes: MutableMap<Name.EntityName, Pair<Int, Instant>> = mutableMapOf()

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
    fun increaseChangeCount(entity: Name.EntityName): Unit {
        val (count, timestamp) = this.changes.getOrDefault(entity, Pair(0, Instant.now())) // get current count and timestamp or exchange it with the default Pair specified here

        // Check if this change will trigger the task based on number of changes or time passed
        if (count + 1 >= this.changesThreshold || Instant.now().isAfter(timestamp.plusSeconds(this.timestampThreshold))) {
            LOGGER.info("A new task was schedules to recreate statistics for entity " + entity.schemaName + "." + entity.entityName)
            this.schedule(entity) // schedule task for this column
            this.changes[entity] = Pair(0, Instant.now()) // Reset count to 0.
        } else {
            LOGGER.info("Change does not trigger a new task")
            this.changes[entity] = Pair(count + 1, Instant.now()) // increase count by 1 if no task was scheduled
        }
    }

}