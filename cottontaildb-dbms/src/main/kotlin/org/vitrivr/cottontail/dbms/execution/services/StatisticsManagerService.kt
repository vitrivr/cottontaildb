package org.vitrivr.cottontail.dbms.execution.services

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.events.*
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.statistics.metricsCollector.*
import org.vitrivr.cottontail.dbms.statistics.metricsData.ValueMetrics
import org.vitrivr.cottontail.dbms.statistics.storage.ColumnMetrics
import org.vitrivr.cottontail.dbms.statistics.values.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import javax.xml.validation.Schema
import kotlin.math.min

/**
 * A [TransactionObserver] that keeps track of different Columns and triggers an analysis for the columns that have reached a specific threshold.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
class StatisticsManagerService(private val catalogue: DefaultCatalogue, private val manager: TransactionManager): TransactionObserver {

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
        return event is DataEvent || event is SchemaEvent
    }

    /**
     * Processes incoming [IndexEvent] and determines which [Index] require re-building.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun onCommit(txId: TransactionId, events: List<Event>) {
        for (event in events) {
            when (event) {
                is DataEvent -> {
                    val numberOfEntries = this.numberOfEntriesOfDataEvent(event)
                    this.updateStatisticsOfEntity(event.entity, numberOfEntries)
                }
                is SchemaEvent -> {
                    this.handleSchemaEvent(event)
                }
                else -> {} // do nothing for all other events
            }
        }
    }

    /**
     * Processes the schema events
     */
    private fun handleSchemaEvent(event: SchemaEvent) {
        when (event) {
            is SchemaEvent.Create -> {
                TODO("To be implemented")
            }
            is SchemaEvent.Drop -> {
                TODO("To be implemented")
            }
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
     * @param entity The [Name.EntityName] of the [Entity] to analyse.
     */
    private fun schedule(entity: Name.EntityName, numberOfEntries : Long) {
        var task: Runnable = AnalysisTask(entity, numberOfEntries)
        this.manager.executionManager.serviceWorkerPool.schedule(task, 100L, TimeUnit.MILLISECONDS)
    }

    /**
     * The actual [Runnable] that executes [Column] analysis.
     */
    inner class AnalysisTask(private val entityName: Name.EntityName, private val numberOfEntries: Long): Runnable {
        override fun run() {
            StatisticsManagerService.LOGGER.info("Starting Task to analyse an entity.")
            val transaction = this@StatisticsManagerService.manager.startTransaction(TransactionType.SYSTEM_READONLY) // It's a read only transaction of the main data
            val statisticsTransaction = this@StatisticsManagerService.catalogue.statisticsEnvironment.beginExclusiveTransaction() // But an exclusive transaction for the statistics part
            val context = DefaultQueryContext("statistics-manager-${this@StatisticsManagerService.counter.incrementAndGet()}", this@StatisticsManagerService.catalogue, transaction)
            try {
                StatisticsManagerService.LOGGER.info("Starting analysis of entity $entityName...")
                val catalogueTx = this@StatisticsManagerService.catalogue.newTx(context)
                val schema = catalogueTx.schemaForName(this.entityName.schema() ?: return)
                val schemaTx = schema.newTx(context)
                val entity = schemaTx.entityForName(this.entityName ?: return)
                val entityTx = entity.newTx(context)
                val columns = entityTx.listColumns().toTypedArray()

                // get the collectors for all columns and give them the numberOfEntries to init the BloomFilter
                val columnsCollector = columns.map { columnDef ->
                    getCollector(columnDef, this@StatisticsManagerService.catalogue.config.statistics, numberOfEntries)
                }

                // Define random properties for skipping some rows of the entity -> only take a sample of the database
                val probability = this@StatisticsManagerService.catalogue.config.statistics.probability
                val randomNumberGenerator = this@StatisticsManagerService.catalogue.config.statistics.randomNumberGenerator

                // create cursor for all columns of this entity and iterate over all of them
                val entityCursor = entityTx.cursor(columns)
                entityCursor.use { cursor ->
                    while (cursor.moveNext()) {
                        if (randomNumberGenerator.nextDouble() <= probability) { //  will be between 0 and 1 (inclusive of both endpoints)
                            val record = cursor.value()
                            // iterate over columns and send value to corresponding collector
                            columnsCollector.forEachIndexed { i, collector ->
                                val value = record[i]
                                collector.receive(value)
                            }
                        }
                    }
                }

                /** Get [ValueMetrics] for each [Column] and store it for later persistent update */
                val metricsList = columnsCollector.mapIndexed { _, collector ->
                    collector.calculate(probability)
                }

                /** Store the */
                columns.forEachIndexed { i, colDef ->
                    val metrics = metricsList[i]
                    val columnMetrics = ColumnMetrics(colDef.name, metrics.type, metrics)
                    this@StatisticsManagerService.catalogue.statisticsStorageManager.updatePersistently(columnMetrics, statisticsTransaction)
                }

                // Commit both transactions if succeeded
                transaction.commit()
                statisticsTransaction.commit()

            } catch (e: Throwable) {
                when (e) {
                    is DatabaseException.SchemaDoesNotExistException,
                    is DatabaseException.EntityAlreadyExistsException,
                    is DatabaseException.ColumnDoesNotExistException -> StatisticsManagerService.LOGGER.error("Statistics Manager analysis of entity $entityName failed because DBO no longer exists.")
                    else -> StatisticsManagerService.LOGGER.error("Statistics Manager analysis of entity $entityName failed due to exception: ${e.message}.")
                }
                transaction.rollback()
                statisticsTransaction.abort()
            }
        }
    }

    /**
     * Function that, based on the [ColumnDef]'s [Types] returns the corresponding [MetricsCollector]
     */
    fun getCollector(def: ColumnDef<*>, statisticsConfig: StatisticsConfig, numberOfEntries: Long) : MetricsCollector<*> {
        var numberOfEntries = numberOfEntries.toInt()
        val collector = when (def.type) {
            Types.Boolean -> BooleanMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Byte -> ByteMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Short -> ShortMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Date -> DateMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Double -> DoubleMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Float -> FloatMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Int -> IntMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Long -> LongMetricsColelctor(statisticsConfig, numberOfEntries)
            Types.String -> StringMetricsCollector(statisticsConfig, numberOfEntries)
            Types.ByteString -> ByteStringMetricsCollector(statisticsConfig, numberOfEntries)
            Types.Complex32 -> Complex32MetricsCollector(statisticsConfig, numberOfEntries)
            Types.Complex64 -> Complex64MetricsCollector(statisticsConfig, numberOfEntries)
            is Types.BooleanVector -> BooleanVectorMetricsCollector(def.type.logicalSize, statisticsConfig, numberOfEntries)
            is Types.DoubleVector -> DoubleVectorMetricsCollector(def.type.logicalSize, statisticsConfig, numberOfEntries)
            is Types.FloatVector -> FloatVectorMetricsCollector(def.type.logicalSize, statisticsConfig, numberOfEntries)
            is Types.IntVector -> IntVectorMetricsCollector(def.type.logicalSize, statisticsConfig, numberOfEntries)
            is Types.LongVector -> LongVectorMetricsCollector(def.type.logicalSize,statisticsConfig, numberOfEntries)
            is Types.Complex32Vector -> Complex32VectorMetricsCollector(def.type.logicalSize, statisticsConfig, numberOfEntries)
            is Types.Complex64Vector -> Complex64VectorMetricsCollector(def.type.logicalSize, statisticsConfig, numberOfEntries)
            else -> throw IllegalArgumentException("Invalid column type")
        }
        return collector
    }

    /**
     * changes is a temporary data struct that tracks the number of changes (and when the last change occurred) in memory
     * Basically in the form of EntityName: <500 Changes, Timestamp>
     */
    private val changesMap: MutableMap<Name.EntityName, Int> = mutableMapOf()

    /**
     * This function updates the statistics/metrics of every column of an entity.
     * But it does so only when a specific Threshold is reached. If you want to force an update the numberOfEntries parameter can just be omitted
     */
    fun updateStatisticsOfEntity(entity: Name.EntityName, numberOfEntries : Long = 0L) {

        val changes = this.changesMap.getOrDefault(entity, 0) // get current count of changes for this entity or init with 0
        val threshold = catalogue.config.statistics.threshold


        if (changes + 1 >= threshold * numberOfEntries) {
            LOGGER.info("A new task was schedules to recreate statistics for entity " + entity.schemaName + "." + entity.entityName)
            this.schedule(entity, numberOfEntries) // schedule task for this column
            this.changesMap[entity] = 0 // Reset count to 0.
        } else {
            LOGGER.info("Change does not trigger a new task for entity " + entity.schemaName + "." + entity.entityName)
            this.changesMap[entity] = changes + 1 // increase count by 1 if no task was scheduled
        }
    }

    /**
     * This function gets the number of entries for an entity and returns them
     */
    private fun numberOfEntriesOfDataEvent(dataEvent: DataEvent) : Long {

        // get
        val columnDefs = dataEvent.data
        var minNumberOfEntries = Long.MAX_VALUE
        for (key in columnDefs.keys) {
            val columnMetrics = this@StatisticsManagerService.catalogue.statisticsStorageManager[key.name]
            minNumberOfEntries = min(minNumberOfEntries, columnMetrics?.statistics?.numberOfEntries ?: 0L)
        }

        return if (minNumberOfEntries != Long.MAX_VALUE) minNumberOfEntries else 0L

    }

}