package org.vitrivr.cottontail.dbms.execution.services

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
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
import org.vitrivr.cottontail.dbms.statistics.metricsData.*
import org.vitrivr.cottontail.dbms.statistics.storage.ColumnMetrics
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min
import kotlin.reflect.typeOf

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
        return event is DataEvent || event is EntityEvent
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
                is EntityEvent -> this.handleEntityEvent(event)
                else -> Unit // do nothing for all other events
            }
        }
    }

    /**
     * Processes the [EntityEvent]s
     */
    private fun handleEntityEvent(event: EntityEvent) {
        when (event) {
            // Same thing will happen when creating a new one or truncating old entities.
            is EntityEvent.Create, is EntityEvent.Truncate -> {
                // Create a task that initializes all the statistics
                val task: Runnable = CreateEntityStatistics(event)
                this.manager.executionManager.serviceWorkerPool.schedule(task, 0, TimeUnit.MILLISECONDS)

                // start transaction for MetaData
                val statisticsTransaction = this@StatisticsManagerService.catalogue.statisticsEnvironment.beginExclusiveTransaction()
                try {
                    // reset change count of newly created entity back to 0
                    this@StatisticsManagerService.catalogue.metaDataStatisticsStorage.resetEntityChanges(event.name, statisticsTransaction)

                    // Commit
                    statisticsTransaction.commit()

                } catch (e: Throwable) {
                    statisticsTransaction.abort()
                }

            }
            is EntityEvent.Drop -> {
                // create task to delete all statistics of this entity
                val task: Runnable = DropEntityStatistics(event)
                this.manager.executionManager.serviceWorkerPool.schedule(task, 0, TimeUnit.MILLISECONDS)

                // start transaction for MetaData
                val statisticsTransaction = this@StatisticsManagerService.catalogue.statisticsEnvironment.beginExclusiveTransaction()
                try {
                    // Delete change count of dropped created entity
                    this@StatisticsManagerService.catalogue.metaDataStatisticsStorage.deleteEntity(event.name, statisticsTransaction)

                    // Commit
                    statisticsTransaction.commit()

                } catch (e: Throwable) {
                    statisticsTransaction.abort()
                }
            }
        }
    }

    /**
     * The [Runnable] that removes statistics from all columns of [Name.EntityName] because it was dropped from the database.
     */
    inner class DropEntityStatistics(private val event: EntityEvent): Runnable {
        override fun run() {
            StatisticsManagerService.LOGGER.info("Starting Task to delete statistics of an entity.")

            // Start statistics transaction
            val statisticsTransaction = this@StatisticsManagerService.catalogue.statisticsEnvironment.beginExclusiveTransaction() // But an exclusive transaction for the statistics part

            try {

                // Delete all column statistics of this entity
                event.columns.forEach { colDef ->
                    this@StatisticsManagerService.catalogue.statisticsStorageManager.deletePersistently(
                        colDef.name,
                        statisticsTransaction
                    )
                }

                // commit
                statisticsTransaction.commit()
            } catch (e: Throwable) {
                StatisticsManagerService.LOGGER.error("Statistics Manager Drop Entity Event of entity ${event.name}: Failed to delete statistics.")
                statisticsTransaction.abort()
            }
        }
    }

    /**
     * The [Runnable] that initializes statistics from all columns of newly created [Name.EntityName].
     */
    inner class CreateEntityStatistics(private val event: EntityEvent): Runnable {
        override fun run() {
            StatisticsManagerService.LOGGER.info("Starting Task to delete statistics of an entity.")

            // Start statistics transaction
            val statisticsTransaction = this@StatisticsManagerService.catalogue.statisticsEnvironment.beginExclusiveTransaction() // But an exclusive transaction for the statistics part

            try {

                // Delete all column statistics of this entity
                event.columns.forEach { colDef ->
                    val metrics = ColumnMetrics(colDef)
                    this@StatisticsManagerService.catalogue.statisticsStorageManager.updatePersistently(
                        metrics,
                        statisticsTransaction
                    )
                }

                // commit
                statisticsTransaction.commit()
            } catch (e: Throwable) {
                StatisticsManagerService.LOGGER.error("Statistics Manager Create Entity Event of entity ${event.name} failed.")
                statisticsTransaction.abort()
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
     * Schedules a new [Runnable] for analysing the specified column.
     *
     * @param entity The [Name.EntityName] of the [Name.EntityName] to analyse.
     */
    private fun schedule(entity: Name.EntityName, numberOfEntries : Long) {
        val task: Runnable = AnalysisTask(entity, numberOfEntries)
        this.manager.executionManager.serviceWorkerPool.schedule(task, 0, TimeUnit.MILLISECONDS)
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

                // Define random properties for skipping some rows of the entity -> only take a sample of the database
                val probability = this@StatisticsManagerService.catalogue.config.statistics.probability
                val randomNumberGenerator = this@StatisticsManagerService.catalogue.config.statistics.randomGenerator

                // get the collectors for all columns and give them the numberOfEntries to init the BloomFilter
                // numberOfEntries is multiplied with probability since the Bloomfilter is only getting a sample of the whole set
                // init list of collectors
                val columnsCollector = mutableListOf<MetricsCollector<*>>()
                // Fill this list
                columns.forEach { columnDef ->
                    val collector = getCollector(columnDef, this@StatisticsManagerService.catalogue.config.statistics, (probability * numberOfEntries).toLong())
                    columnsCollector.add(collector)
                }

                // create cursor for all columns of this entity and iterate over all of them
                val entityCursor = entityTx.cursor(columns)
                entityCursor.use { cursor ->
                    while (cursor.moveNext()) {
                        if (randomNumberGenerator.nextDouble(0.0, 1.0) <= probability || numberOfEntries < this@StatisticsManagerService.catalogue.config.statistics.minSampleSize) { //  will be between 0 and 1 (inclusive of both endpoints)
                            val record = cursor.value()
                            // iterate over columns and send value to corresponding collector
                            columnsCollector.forEachIndexed { i, collector ->
                                when (val value = record[i]) {
                                    is BooleanValue -> (collector as BooleanMetricsCollector).receive(value)
                                    is BooleanVectorValue -> (collector as BooleanVectorMetricsCollector).receive(value)
                                    is ByteValue -> (collector as ByteMetricsCollector).receive(value)
                                    is ByteStringValue -> (collector as ByteStringMetricsCollector).receive(value)
                                    is Complex32Value -> (collector as Complex32MetricsCollector).receive(value)
                                    is Complex32VectorValue -> (collector as Complex32VectorMetricsCollector).receive(value)
                                    is Complex64Value -> (collector as Complex64MetricsCollector).receive(value)
                                    is Complex64VectorValue -> (collector as Complex64VectorMetricsCollector).receive(value)
                                    is DateValue -> (collector as DateMetricsCollector).receive(value)
                                    is DoubleValue -> (collector as DoubleMetricsCollector).receive(value)
                                    is DoubleVectorValue -> (collector as DoubleVectorMetricsCollector).receive(value)
                                    is FloatValue -> (collector as FloatMetricsCollector).receive(value)
                                    is FloatVectorValue -> (collector as FloatVectorMetricsCollector).receive(value)
                                    is IntValue -> (collector as IntMetricsCollector).receive(value)
                                    is IntVectorValue -> (collector as IntVectorMetricsCollector).receive(value)
                                    is LongValue -> (collector as LongMetricsColelctor).receive(value)
                                    is LongVectorValue -> (collector as LongVectorMetricsCollector).receive(value)
                                    is StringValue -> (collector as StringMetricsCollector).receive(value)
                                    else -> collector.receive(null) // Just give them null  value for unknown type
                                }
                            }
                        }
                    }
                }

                /** Get [ValueMetrics] for each [Column] and store it for later persistent update */
                val metricsList = columnsCollector.mapIndexed { _, collector ->
                    collector.calculate(probability)
                }

                /** Persistently Store the metrics */
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
        val numberOfEntries = numberOfEntries.toInt()
        val config = MetricsConfig(statisticsConfig, numberOfEntries)
        val collector = when (def.type) {
            Types.Boolean -> BooleanMetricsCollector(config)
            Types.Byte -> ByteMetricsCollector(config)
            Types.Short -> ShortMetricsCollector(config)
            Types.Date -> DateMetricsCollector(config)
            Types.Double -> DoubleMetricsCollector(config)
            Types.Float -> FloatMetricsCollector(config)
            Types.Int -> IntMetricsCollector(config)
            Types.Long -> LongMetricsColelctor(config)
            Types.String -> StringMetricsCollector(config)
            Types.ByteString -> ByteStringMetricsCollector(config)
            Types.Complex32 -> Complex32MetricsCollector(config)
            Types.Complex64 -> Complex64MetricsCollector(config)
            is Types.BooleanVector -> BooleanVectorMetricsCollector(def.type.logicalSize, config)
            is Types.DoubleVector -> DoubleVectorMetricsCollector(def.type.logicalSize, config)
            is Types.FloatVector -> FloatVectorMetricsCollector(def.type.logicalSize, config)
            is Types.IntVector -> IntVectorMetricsCollector(def.type.logicalSize, config)
            is Types.LongVector -> LongVectorMetricsCollector(def.type.logicalSize, config)
            is Types.Complex32Vector -> Complex32VectorMetricsCollector(def.type.logicalSize, config)
            is Types.Complex64Vector -> Complex64VectorMetricsCollector(def.type.logicalSize, config)
            else -> throw IllegalArgumentException("Invalid column type")
        }
        return collector
    }

    /**
     * This function updates the statistics/metrics of every column of an entity.
     * But it does so only when a specific Threshold is reached. If you want to force an update the numberOfEntries parameter can just be omitted
     */
    fun updateStatisticsOfEntity(entity: Name.EntityName, numberOfEntries : Long = 0L) {

        // Get Threshold from config
        val threshold = catalogue.config.statistics.threshold

        // start transaction for MetaData
        val statisticsTransaction = this@StatisticsManagerService.catalogue.statisticsEnvironment.beginExclusiveTransaction()

        // Try transaction or rollback
        try {
            // Get number of changes
            val changes  = this@StatisticsManagerService.catalogue.metaDataStatisticsStorage[entity]

            if (changes + 1 >= threshold * numberOfEntries) {
                LOGGER.info("A new task was scheduled to recreate statistics for entity " + entity.schemaName + "." + entity.entityName + ", because ${changes + 1} >= ${threshold * numberOfEntries}")

                // schedule task for this column
                this.schedule(entity, numberOfEntries)

                // Reset count of Entity
                this@StatisticsManagerService.catalogue.metaDataStatisticsStorage.resetEntityChanges(entity, statisticsTransaction)

            } else {
                LOGGER.info("Change does not trigger a new task for entity " + entity.schemaName + "." + entity.entityName + ", because ${changes + 1} < ${threshold * numberOfEntries}")

                this@StatisticsManagerService.catalogue.metaDataStatisticsStorage.increaseEntityChanges(entity, statisticsTransaction)
            }

            // Commit MetaData transaction
            statisticsTransaction.commit()
        } catch (e: Throwable) {
            statisticsTransaction.abort()
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