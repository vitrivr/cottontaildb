package org.vitrivr.cottontail.dbms.statistics

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.EntityEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.statistics.collectors.MetricsCollector
import org.vitrivr.cottontail.dbms.statistics.collectors.MetricsConfig
import org.vitrivr.cottontail.dbms.statistics.storage.ColumnStatistic
import org.vitrivr.cottontail.dbms.statistics.storage.EntityMetric
import org.vitrivr.cottontail.dbms.statistics.storage.IndexStatistic
import org.vitrivr.cottontail.dbms.statistics.storage.StatisticsStorageManager
import org.vitrivr.cottontail.server.Instance
import java.io.Closeable
import java.lang.ref.SoftReference
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.abs
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * The [StatisticsManager] is the central entity that manages and provides access to different types of statistics.
 *
 * It acts as  [TransactionObserver] to keep track of changes and to trigger analysis.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.3.0
 */
class StatisticsManager(private val instance: Instance): TransactionObserver, Closeable {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(StatisticsManager::class.java)
    }

    /** Internal counter to keep track of then number of spawned tasks. */
    private val counter = AtomicLong(0L)

    /** The statistics/metrics Xodus [Environment] used by the [StatisticsManager]. */
    private val store = StatisticsStorageManager(this.instance.config)

    /** An in-memory cache for frequently accessed [ColumnStatistic]. */
    private val columnCache = Object2ObjectLinkedOpenHashMap<Name.ColumnName, SoftReference<ColumnStatistic>>()

    /** An in-memory cache for frequently accessed [ColumnStatistic]. */
    private val indexCache = Object2ObjectLinkedOpenHashMap<Name.IndexName, SoftReference<List<IndexStatistic>>>()

    /** A queue of [Name.EntityName] that require new statistics */
    private val queue = Collections.synchronizedSet(LinkedHashSet<Name.EntityName>())

    init {
        this.instance.executor.serviceWorkerPool.scheduleAtFixedRate(Collector(), 10, 10, TimeUnit.SECONDS)
    }

    /**
     * The [StatisticsManager] is interested in all [DataEvent]'s.
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
        val updated = HashMap<Name.EntityName, EntityMetric>()
        for (event in events) {
            when (event) {
                /* Creates all the column statistics and the entity metrics entry. */
                is EntityEvent.Create -> event.columns.forEach {
                    this.store[it.name] = ColumnStatistic(it)
                    this.store.setMetric(event.entity.name, EntityMetric())
                }

                /* Deletes all the column statistics and the entity metrics entry, whenever an entity is dropped. */
                is EntityEvent.Drop -> {
                    event.columns.forEach { this.store.deleteColumnStatistic(it.name) }
                    this.store.deleteMetric(event.entity.name) /* Removes all metrics collected for the entity .*/
                }

                /* Resets all the column statistics and the entity metrics entry, whenever an entity is truncated. */
                is EntityEvent.Truncate -> {
                    event.columns.forEach { this.store[it.name] = ColumnStatistic(it) }
                    this.store.setMetric(event.entity.name, EntityMetric())
                }

                /* Updates metrics for an entity, whenever there is a change to the data. */
                is DataEvent.Insert -> updated.compute(event.entity) { k, v ->
                    val metric = v ?: (this.store.getMetric(k) ?: EntityMetric())
                    metric.inserts += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }

                /* Updates metrics for an entity, whenever there is a change to the data. */
                is DataEvent.Update -> updated.compute(event.entity) { k, v ->
                    val metric = v ?: (this.store.getMetric(k) ?: EntityMetric())
                    metric.updates += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }

                /* Updates metrics for an entity, whenever there is a change to the data. */
                is DataEvent.Delete -> updated.compute(event.entity) { k, v ->
                    val metric = v ?: (this.store.getMetric(k) ?: EntityMetric())
                    metric.deletes += 1
                    metric.deltaSinceAnalysis += 1
                    metric
                }
                else -> { /* no op. */ }
            }
        }

        /* Store updated metric and schedule analyser task if accumulated changes exceed configured threshold.  */
        for ((entity, metric) in updated) {
            this.store.setMetric(entity, metric)
            if (metric.lastAnalysis == 0L || abs(metric.deltaSinceAnalysis.toFloat() / metric.total.toFloat()) >= this@StatisticsManager.instance.config.statistics.threshold) {
                this.queue.add(entity)
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
        var metric = this.columnCache[name]?.get()
        if (metric != null) return metric
        metric = this.store[name]
        if (metric == null) return null
        this.columnCache[name] = SoftReference(metric)
        return metric
    }

    /**
     * Tries to access and return an up-to-date [IndexStatistic] object for the given [Name.IndexName].
     *
     * @param name [Name.IndexName] The [Name.IndexName] to obtain [IndexStatistic] for.
     * @return [IndexStatistic]
     */
    operator fun get(name: Name.IndexName): List<IndexStatistic> {
        var metric = this.indexCache[name]?.get()
        if (metric != null) return metric
        metric = this.store[name]
        this.indexCache[name] = SoftReference(metric)
        return metric
    }

    /**
     * Tries to update[IndexStatistic] object for the given [Name.IndexName].
     *
     * @param name [Name.IndexName] The [Name.IndexName] to update [IndexStatistic] for.
     * @param statistics Updated [IndexStatistic]
     */
    operator fun set(name: Name.IndexName, statistics: List<IndexStatistic>) {
        this.store[name] = statistics
        this.indexCache[name] = SoftReference(statistics)
    }

    /**
     * Starts gathering new statistics for the specified [Name.EntityName]. This is a blocking method!
     *
     * @param name The [Name.EntityName] to update statistics for.
     */
    @OptIn(ExperimentalTime::class)
    @Suppress("UNCHECKED_CAST")
    fun gatherStatisticsForEntity(name: Name.EntityName) {
        LOGGER.info("Starting statistics gathering for entity $name.")

        /* Log progress. */
        val transaction = this@StatisticsManager.instance.transactions.startTransaction(TransactionType.SYSTEM_READONLY)
        val context = DefaultQueryContext("statistics-manager-${this@StatisticsManager.counter.incrementAndGet()}", this@StatisticsManager.instance, transaction)
        try {
            val schema = context.catalogueTx.schemaForName(name.schema())
            val schemaTx = schema.newTx(context.catalogueTx)
            val entity = schemaTx.entityForName(name)
            val entityTx = entity.createOrResumeTx(schemaTx)
            val columns = entityTx.listColumns().toTypedArray()

            /* Determines the number of entries that must be scanned. */
            val count = entityTx.count()
            val sampleProbability = this@StatisticsManager.instance.config.statistics.sampleProbability
            val sampledEntries = (count * sampleProbability).toLong()
            val config = if (sampledEntries <= this@StatisticsManager.instance.config.statistics.minimumSampleSize) {
                MetricsConfig(this@StatisticsManager.instance.config.statistics, count, 1.0f)
            } else {
                MetricsConfig(this@StatisticsManager.instance.config.statistics, sampledEntries, sampleProbability)
            }


            /* Prepares array of column data collectors. */
            val collectors = Array(columns.size) {
                MetricsCollector.collectorForType(columns[it].type, config) as MetricsCollector<Value>
            }

            /* Scans the data and passes it to the collector */
            val duration = measureTime {
                entityTx.cursor().use { cursor ->
                    if (sampledEntries <= this@StatisticsManager.instance.config.statistics.minimumSampleSize) {
                        while (cursor.moveNext()) {
                            val record = cursor.value()
                            collectors.forEachIndexed { index, collect -> collect.receive(record[index]) }
                        }
                    } else {
                        val generator = this@StatisticsManager.instance.config.statistics.randomGenerator()
                        while (cursor.moveNext()) {
                            if (generator.nextFloat(0.0f, 1.0f) <= sampleProbability) {
                                val record = cursor.value()
                                collectors.forEachIndexed { index, collect -> collect.receive(record[index]) }
                            }
                        }
                    }
                }

                /* Now obtain the statistics and store them persistently. */
                for ((column, collector) in columns.zip(collectors)) {
                    this@StatisticsManager.store[column.name] = ColumnStatistic(column.type, collector.calculate())
                    this@StatisticsManager.columnCache.remove(column.name) /* Invalidates entry in cache. */
                }

                /* Updates the metrics for the entity. */
                val metric = this@StatisticsManager.store.getMetric(name) ?: EntityMetric()
                metric.deltaSinceAnalysis = 0L
                metric.lastAnalysis = System.currentTimeMillis()
                this@StatisticsManager.store.setMetric(name, metric)
            }

            LOGGER.info("Statistics gathering for entity $name completed successfully (took $duration).")
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            LOGGER.error("Statistics manager failed to update entity statistics because schema ${name.schema()} does not exist.")
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            LOGGER.error("Statistics manager failed to update entity statistics because entity $name does not exist.")
        } catch (e: Throwable) {
            LOGGER.error("Statistics manager failed to update entity statistics due to exception: ${e.message}.")
        } finally {
            transaction.abort()
        }
    }

    /**
     * A [Runnable] that gathers statistics for [Name.EntityName]s listed in the [StatisticsManager.queue] at a regular interval.
     */
    private inner class Collector: Runnable {
        /**
         * Run method.
         */
        override fun run() {
           /* Fetch next entry. */
           val next = this@StatisticsManager.queue.firstOrNull()
           if (next != null) {
               this@StatisticsManager.queue.remove(next)
               this@StatisticsManager.gatherStatisticsForEntity(next)
           }
        }
    }

    /**
     * Closes this [StatisticsManager].
     */
    override fun close() {
        this.store.close()
    }
}