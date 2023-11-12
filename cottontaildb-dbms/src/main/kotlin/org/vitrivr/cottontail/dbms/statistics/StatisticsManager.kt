package org.vitrivr.cottontail.dbms.statistics

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.events.*
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.statistics.collectors.*
import org.vitrivr.cottontail.dbms.statistics.storage.*
import java.io.Closeable
import java.lang.ref.SoftReference
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * The [StatisticsManager] is the central entity that manages and provides access to different types of statistics.
 *
 * It acts as  [TransactionObserver] to keep track of changes and to trigger analysis.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.0.0
 */
class StatisticsManager(private val config: StatisticsConfig, private val manager: TransactionManager): TransactionObserver, Closeable {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(StatisticsManager::class.java)
    }

    /** The statistics/metrics Xodus [Environment] used by the [ColumnStatisticsStore]. */
    private val environment: Environment = Environments.newInstance(this.manager.config.statisticsFolder().toFile(), this.manager.config.xodus.toEnvironmentConfig())

    /** The [ColumnStatisticsStore] wraps a Xodus [Store] and can be used to store / obtain [ColumnStatistic]. */
    private val columnsStatisticsStore: ColumnStatisticsStore

    /** The [IndexStatisticsStore] wraps a Xodus [Store] and can be used to store / obtain [IndexStatistic]. */
    private val indexStatisticsStore: IndexStatisticsStore

    /** The [EntityMetricsStore] wraps a Xodus [Store] and can be used to store / obtain [EntityMetric]. */
    private val entityMetricsStore: EntityMetricsStore

    init{
        val tx = this.environment.beginExclusiveTransaction()
        try {
            this.columnsStatisticsStore = ColumnStatisticsStore(this.environment.openStore("org.vitrivr.cottontail.statistics.column", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx))
            this.indexStatisticsStore = IndexStatisticsStore(this.environment.openStore("org.vitrivr.cottontail.statistics.index", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx))
            this.entityMetricsStore = EntityMetricsStore(this.environment.openStore("org.vitrivr.cottontail.statistics.entity", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx))
        } finally {
            tx.commit()
        }
    }

    /** An in-memory cache for frequently accessed [ColumnStatistic]. */
    private val columnStatisticsCache = ConcurrentHashMap<Name.ColumnName, SoftReference<ColumnStatistic>>()

    /** Internal [ConcurrentHashMap] of all statistics items. */
    private val indexStatisticsCache = Object2ObjectLinkedOpenHashMap<Name.IndexName, SoftReference<IndexStatistic>>()

    /**
     * The [StatisticsManager] is interested in all [ColumnEvent]'s.
     *
     * @param event The [Event] to check.
     * @return True
     */
    override fun isRelevant(event: Event): Boolean = event is DataEvent || event is EntityEvent || event is IndexEvent

    /**
     * Processes incoming [Event]s.
     *
     * The [StatisticsManager] acts upon [EntityEvent], [IndexEvent] and [DataEvent] to keep track of and update statistics and metrics.
     *
     * @param txId [TransactionId] that signals its [Event]s.
     * @param events The list of [Event]s in order at which they were applied.
     */
    override fun onCommit(txId: TransactionId, events: List<Event>) = this.environment.executeInExclusiveTransaction { tx ->
        val metrics = HashMap<Name.EntityName, EntityMetric>()
        for (event in events) {
            when (event) {
                /* Creates all column and entity statistics whenever an entity is created. */
                is EntityEvent.Create -> {
                    this.entityMetricsStore[tx, event.name] = EntityMetric()
                    event.columns.forEach {
                        this.columnsStatisticsStore[tx, it.name] = ColumnStatistic(it)
                        this.columnStatisticsCache.remove(it.name) /* Invalidates entry in cache. */
                    }
                }

                /* Resets all the column and entity statistics, when an entity is truncated. */
                is EntityEvent.Truncate -> {
                    this.entityMetricsStore[tx, event.name] = EntityMetric()
                    event.columns.forEach {
                        this.columnsStatisticsStore[tx, it.name] = ColumnStatistic(it)
                        this.columnStatisticsCache.remove(it.name) /* Invalidates entry in cache. */
                    }
                }

                /* Deletes all the column statistics, whenever an entity is dropped and removes all collected metrics. */
                is EntityEvent.Drop -> {
                    this.entityMetricsStore.delete(tx, event.name) /* Removes all metrics collected for the entity .*/
                    event.columns.forEach {
                        this.columnsStatisticsStore.delete(tx, it.name)
                        this.columnStatisticsCache.remove(it.name) /* Invalidates entry in cache. */
                    }
                }

                /* Removes all index statistics when an index is dropped. */
                is IndexEvent.Dropped -> {
                    this.indexStatisticsStore.delete(tx, event.index)
                    this.indexStatisticsCache.remove(event.index) /* Invalidates entry in cache. */
                }

                /* Updates metrics for an entity, whenever there is a change to the data. */
                is DataEvent.Insert -> metrics.compute(event.entity) { k, v ->
                    val metric = v ?: (this.entityMetricsStore[tx, k] ?: EntityMetric())
                    metric.inserts += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                is DataEvent.Update -> metrics.compute(event.entity) { k, v ->
                    val metric = v ?: (this.entityMetricsStore[tx, k] ?: EntityMetric())
                    metric.updates += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                is DataEvent.Delete -> metrics.compute(event.entity) { k, v ->
                    val metric = v ?: (this.entityMetricsStore[tx, k] ?: EntityMetric())
                    metric.deletes += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                else -> { /* no op. */ }
            }
        }

        /* Store updated metric and schedule analyser task if accumulated changes exceed configured threshold.  */
        for ((entity, metric) in metrics) {
            this.entityMetricsStore[tx, entity] = metric
            if (metric.deltaSinceAnalysis >= this@StatisticsManager.manager.config.statistics.threshold) {
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
        var metric = this.columnStatisticsCache[name]?.get()
        if (metric != null) return metric
        this.environment.computeInReadonlyTransaction { tx ->
            metric = this.columnsStatisticsStore[tx, name]
        }
        if (metric == null) return null
        this.columnStatisticsCache[name] = SoftReference(metric)
        return metric
    }

    /**
     * Tries to access and return an up-to-date [EntityMetric] object for the given [Name.ColumnName].
     *
     * @param name [Name.EntityName] The [Name.EntityName] to obtain [EntityMetric] for.
     * @return [EntityMetric]
     */
    operator fun get(name: Name.EntityName): EntityMetric? {
        return this.environment.computeInReadonlyTransaction { tx ->
            this.entityMetricsStore[tx, name]
        }
    }

    /**
     * Tries to access and return an up-to-date [IndexStatistic] object for the given [Name.IndexName].
     *
     * @param name [Name.IndexName] The [Name.IndexName] to obtain [IndexStatistic] for.
     * @return [IndexStatistic]
     */
    operator fun get(name: Name.IndexName): IndexStatistic? {
        var statistic = this.indexStatisticsCache[name]?.get()
        if (statistic != null) return statistic
        this.environment.computeInReadonlyTransaction { tx ->
            statistic = this.indexStatisticsStore[tx, name]
        }
        if (statistic == null) return null
        this.indexStatisticsCache[name] = SoftReference(statistic)
        return statistic
    }

    /**
     * Updates te [IndexStatistic] for the given [Name.IndexName].
     *
     * @param name [Name.IndexName] to update [IndexStatistic] for
     * @param statistics The new [IndexStatistic]
     * @return [IndexStatistic]
     */
    operator fun set(name: Name.IndexName, statistics: IndexStatistic) {
        this.environment.computeInExclusiveTransaction { tx ->
            this.indexStatisticsStore[tx, name] = statistics
        }
        this.indexStatisticsCache[name] = SoftReference(statistics)
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

        val transaction = this@StatisticsManager.manager.SnapshotReadonly()
        try {
            val duration = measureTime {
                val entityTx = transaction.entityTx(entityName, AccessMode.READ)
                val columns = entityTx.listColumns().toTypedArray()

                /* Determines the number of entries that must be scanned. */
                val sampleProbability = this@StatisticsManager.config.sampleProbability
                val expectedEntries = (entityTx.count() * sampleProbability).toLong()

                /* Prepares array of column data collectors. */
                val collectors = Array(columns.size) {
                    getCollector(columns[it], this@StatisticsManager.config, expectedEntries) as MetricsCollector<Value>
                }

                /* Scans the data and passes it to the collector */
                entityTx.cursor(columns).use { cursor ->
                    if (expectedEntries <= this@StatisticsManager.config.minimumSampleSize) {
                        while (cursor.moveNext()) {
                            val record = cursor.value()
                            collectors.forEachIndexed { index, collect ->  collect.receive(record[index]) }
                        }
                    } else {
                        val generator = this@StatisticsManager.config.randomGenerator()
                        while (cursor.moveNext()) {
                            if (generator.nextDouble(0.0, 1.0) <= sampleProbability) {
                                val record = cursor.value()
                                collectors.forEachIndexed { index, collect ->  collect.receive(record[index]) }
                            }
                        }
                    }
                }

                this@StatisticsManager.environment.computeInExclusiveTransaction { tx ->
                    /* Now obtain the statistics and store them persistently. */
                    for ((column, collector) in columns.zip(collectors)) {
                        this@StatisticsManager.columnsStatisticsStore[tx, column.name] = ColumnStatistic(column.type, collector.calculate(sampleProbability))
                        this@StatisticsManager.columnStatisticsCache.remove(column.name) /* Invalidates entry in cache. */
                    }

                    /* Updates the metrics for the entity. */
                    val metric = this@StatisticsManager.entityMetricsStore[tx, entityName] ?: EntityMetric()
                    metric.deltaSinceAnalysis = 0L
                    metric.lastAnalysis = System.currentTimeMillis()
                    this@StatisticsManager.entityMetricsStore[tx, entityName] = metric
                }
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

    /**
     * Closes this [StatisticsManager]
     */
    override fun close() = this.environment.close()
}