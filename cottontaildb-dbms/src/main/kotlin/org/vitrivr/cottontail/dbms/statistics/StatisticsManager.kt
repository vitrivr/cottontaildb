package org.vitrivr.cottontail.dbms.statistics

import jetbrains.exodus.env.Environment
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.events.*
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.statistics.collectors.*
import org.vitrivr.cottontail.dbms.statistics.metrics.EntityMetric
import org.vitrivr.cottontail.dbms.statistics.storage.ColumnStatistic
import org.vitrivr.cottontail.dbms.statistics.storage.StatisticsStorageManager
import java.lang.ref.SoftReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * The [StatisticsManager] is the central entity that manages and provides access to different types of statistics.
 *
 * It acts as  [TransactionObserver] to keep track of changes and to trigger analysis.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
class StatisticsManager(private val catalogue: DefaultCatalogue, private val manager: TransactionManager): TransactionObserver {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(StatisticsManager::class.java)
    }

    /** Internal counter to keep track of then number of spawned tasks. */
    private val counter = AtomicLong(0L)

    /** The statistics/metrics Xodus [Environment] used by the [StatisticsManager]. */
    private val store = StatisticsStorageManager(this.catalogue.config)

    /** An in-memory cache for frequently accessed [ColumnStatistic]. */
    private val cache = ConcurrentHashMap<Name.ColumnName, SoftReference<ColumnStatistic>>()

    /**
     * The [StatisticsManager] is interested in all [ColumnEvent]'s.
     *
     * @param event The [Event] to check.
     * @return True
     */
    override fun isRelevant(event: Event): Boolean = event is DataEvent || event is EntityEvent

    /**
     * Processes incoming [Event]s. The [StatisticsManager] acts upon [EntityEvent] and [DataEvent] to keep track of and update statistics and metrics.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun onCommit(txId: TransactionId, events: List<Event>) {
        val metrics = HashMap<Name.EntityName,EntityMetric>()
        for (event in events) {
            when (event) {
                /* Creates all the column statistics whenever an entity is created. */
                is EntityEvent.Create -> event.columns.forEach { this.store.setColumnStatistic(it.name, ColumnStatistic(it)) }

                /* Resets all the column statistics, whenever an entity is truncated and removes all collected metrics*/
                is EntityEvent.Truncate -> {
                    event.columns.forEach { this.store.setColumnStatistic(it.name, ColumnStatistic(it)) }
                    this.store.deleteMetric(event.name) /* Removes all metrics collected for the entity .*/
                }
                /* Deletes all the column statistics, whenever an entity is dropped and removes all collected metrics. */
                is EntityEvent.Drop -> {
                    event.columns.forEach { this.store.deleteColumnStatistic(it.name) }
                    this.store.deleteMetric(event.name) /* Removes all metrics collected for the entity .*/
                }
                /* Updates metrics for an entity, whenever there is a change to the data. */
                is DataEvent.Insert -> metrics.compute(event.entity) { k, v ->
                    val metric = v ?: (this.store.getMetric(k) ?: EntityMetric())
                    metric.inserts += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                is DataEvent.Update -> metrics.compute(event.entity) { k, v ->
                    val metric = v ?: (this.store.getMetric(k) ?: EntityMetric())
                    metric.updates += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                is DataEvent.Delete -> metrics.compute(event.entity) { k, v ->
                    val metric = v ?: (this.store.getMetric(k) ?: EntityMetric())
                    metric.deletes += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                else -> { /* no op. */ }
            }
        }

        /* Store updated metric and schedule analyser task if accumulated changes exceed configured threshold.  */
        for ((entity, metric) in metrics) {
            this.store.setMetric(entity, metric)
            if (metric.deltaSinceAnalysis >= this@StatisticsManager.catalogue.config.statistics.threshold) {
                this.manager.executionManager.serviceWorkerPool.schedule(AnalysisTask(entity), 1, TimeUnit.SECONDS)
            }
        }
    }

    /**
     * The [StatisticsManager] does not care about a delivery failure at the moment.
     */
    override fun onDeliveryFailure(txId: TransactionId) {
        /* No op. */
    }

    /**
     * Tries to access and return an up-to-date [ColumnStatistic] object for the given [Name.ColumnName].
     *
     * @param name [Name.ColumnName] The [Name.ColumnName] to obtain [ColumnStatistic] for.
     * @return [ColumnStatistic]
     */
    operator fun get(name: Name.ColumnName): ColumnStatistic? {
        var metric = this.cache[name]?.get()
        if (metric != null) return metric
        metric = this.store[name]
        if (metric == null) return null
        this.cache[name] = SoftReference(metric)
        return metric
    }


    /**
     * Starts gathering new statistics for the specified [Name.EntityName]. This is a blocking method!
     *
     * @param entityName The [Name.EntityName] to update statistics for.
     */
    @OptIn(ExperimentalTime::class)
    @Suppress("UNCHECKED_CAST")
    fun gatherStatisticsForEntity(entityName: Name.EntityName) {
        /* Log progress. */
        LOGGER.info("Starting statistics gathering for entity $entityName.")

        val transaction = this@StatisticsManager.manager.startTransaction(TransactionType.SYSTEM_READONLY)
        val context = DefaultQueryContext("statistics-manager-${this@StatisticsManager.counter.incrementAndGet()}", this@StatisticsManager.catalogue, transaction)
        try {
            val duration = measureTime {
                val catalogueTx = this@StatisticsManager.catalogue.newTx(context)
                val schema = catalogueTx.schemaForName(entityName.schema())
                val schemaTx = schema.newTx(context)
                val entity = schemaTx.entityForName(entityName)
                val entityTx = entity.newTx(context)
                val columns = entityTx.listColumns().toTypedArray()

                /* Determines the number of entries that must be scanned. */
                val sampleProbability = this@StatisticsManager.catalogue.config.statistics.sampleProbability
                val expectedEntries = (entityTx.count() * sampleProbability).toLong()

                /* Prepares array of column data collectors. */
                val collectors = Array(columns.size) {
                    getCollector(columns[it], this@StatisticsManager.catalogue.config.statistics, expectedEntries) as MetricsCollector<Value>
                }

                /* Scans the data and passes it to the collector */
                entityTx.cursor(columns).use { cursor ->
                    if (expectedEntries <= this@StatisticsManager.catalogue.config.statistics.minimumSampleSize) {
                        while (cursor.moveNext()) {
                            val record = cursor.value()
                            collectors.forEachIndexed { index, collect ->  collect.receive(record[index]) }
                        }
                    } else {
                        val generator = this@StatisticsManager.catalogue.config.statistics.randomGenerator()
                        while (cursor.moveNext()) {
                            if (generator.nextDouble(0.0, 1.0) <= sampleProbability) {
                                val record = cursor.value()
                                collectors.forEachIndexed { index, collect ->  collect.receive(record[index]) }
                            }
                        }
                    }
                }

                /* Now obtain the statistics and store them persistently. */
                for ((column, collector) in columns.zip(collectors)) {
                    this@StatisticsManager.store.setColumnStatistic(column.name, ColumnStatistic(column.type, collector.calculate(sampleProbability)))
                    this@StatisticsManager.cache.remove(column.name) /* Invalidates entry in cache. */
                }

                /* Updates the metrics for the entity. */
                val metric = this@StatisticsManager.store.getMetric(entityName) ?: EntityMetric()
                metric.deltaSinceAnalysis = 0L
                metric.lastAnalysis = System.currentTimeMillis()
                this@StatisticsManager.store.setMetric(entityName, metric)
            }

            /* Log progress. */
            LOGGER.info("Statistics gathering for entity $entityName completed successfully (took $duration).")
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            LOGGER.error("Statistics manager failed to update entity statistics because schema ${entityName.schema()} does not exist.")
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            LOGGER.error("Statistics manager failed to update entity statistics because entity $entityName does not exist.")
        } catch (e: Throwable) {
            LOGGER.error("Statistics manager failed to update entity statistics due to exception: ${e.message}.")
        } finally {
            transaction.rollback()
        }
    }

    /**
     * Function that, based on the [ColumnDef]'s [Types] returns the corresponding [MetricsCollector]
     */
    private fun getCollector(def: ColumnDef<*>, statisticsConfig: StatisticsConfig, numberOfEntries: Long) : MetricsCollector<*> {
        val config = MetricsConfig(statisticsConfig, numberOfEntries)
        val collector = when (def.type) {
            Types.Boolean -> BooleanMetricsCollector(config)
            Types.Byte -> ByteMetricsCollector(config)
            Types.Short -> ShortMetricsCollector(config)
            Types.Date -> DateMetricsCollector(config)
            Types.Double -> DoubleMetricsCollector(config)
            Types.Float -> FloatMetricsCollector(config)
            Types.Int -> IntMetricsCollector(config)
            Types.Long -> LongMetricsCollector(config)
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
     * A [Runnable] that executes statistics gathering asynchronously.
     */
    inner class AnalysisTask(private val entityName: Name.EntityName): Runnable {
        override fun run() = this@StatisticsManager.gatherStatisticsForEntity(this.entityName)
    }
}