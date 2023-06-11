package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.statistics.metrics.EntityMetric

/**
 * A class that manages  the storage of [ColumnMetrics] by keeping them in memory and updating their value in the storage when necessary.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
class StatisticsStorageManager(config: Config) {

    /** The statistics/metrics Xodus [Environment] used by the [StatisticsStorageManager]. */
    private val environment: Environment = Environments.newInstance(config.statisticsFolder().toFile())

    /** The Xodus [Store] backing this [StatisticsStorageManager]. */
    private val columnsStore: Store

    /** The Xodus [Store] backing this [StatisticsStorageManager]. */
    private val metricsStore: Store

    init{
        val tx = this.environment.beginReadonlyTransaction()
        try {
            this.columnsStore = this.environment.openStore("org.vitrivr.cottontail.statistics.column", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx)
            this.metricsStore = this.environment.openStore("org.vitrivr.cottontail.statistics.column", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx)
        } finally {
            tx.abort()
        }
    }

    /**
     * Retrieves a statistic [ColumnMetrics] from the map.
     *
     * @param column The key to retrieve the statistics [ColumnMetrics] for.
     */
    operator fun get(column: Name.ColumnName): ColumnMetrics? = this.environment.computeInReadonlyTransaction { tx ->
        this.columnsStore.get(tx, NameBinding.Column.objectToEntry(column))?.let { (ColumnMetrics.Serialized.entryToObject(it) as ColumnMetrics.Serialized).toActual(column) }
    }

    /**
     * Reads and returns an [EntityMetric] for the given [Name.EntityName].
     *
     * @param entity [Name.EntityName] to read [EntityMetric] for.
     * @return [EntityMetric]
     */
    fun getMetric(entity: Name.EntityName): EntityMetric? = this.environment.computeInReadonlyTransaction { tx ->
        this.metricsStore.get(tx, NameBinding.Entity.objectToEntry(entity))?.let { EntityMetric.entryToObject(it) }
    }

    /**
     * Updates the [EntityMetric] for the [Name.EntityName].
     *
     * @param entity [Name.EntityName] to update [EntityMetric] for.
     * @param metric The new [EntityMetric].
     * @return [EntityMetric]
     */
    fun setMetric(entity: Name.EntityName, metric: EntityMetric): Boolean = this.environment.computeInExclusiveTransaction { tx ->
        this.metricsStore.put(tx, NameBinding.Entity.objectToEntry(entity), EntityMetric.objectToEntry(metric))
    }

    /**
     * Deletes [EntityMetric] for the given [Name.EntityName].
     *
     * @param entity [Name.EntityName] to delete [EntityMetric] for.
     * @return True if [EntityMetric] was deleted, false otherwise.
     */
    fun deleteMetric(entity: Name.EntityName): Boolean = this.environment.computeInExclusiveTransaction { tx ->
        this.metricsStore.delete(tx, NameBinding.Entity.objectToEntry(entity))
    }

    /**
     * Updates a statistic [ColumnMetrics] in this [StatisticsStorageManager].
     *
     * @param name The [Name.ColumnName] to set the [ColumnMetrics] for.
     * @param statistic The new [ColumnMetrics].
     */
    fun setColumnStatistic(name: Name.ColumnName, statistic: ColumnMetrics) {
        this.environment.executeInExclusiveTransaction { tx ->
            this.columnsStore.put(tx, NameBinding.Column.objectToEntry(name), ColumnMetrics.Serialized.objectToEntry(statistic.toSerialized())) // write to storage
        }
    }

    /**
     * Removes a [ColumnMetrics] from this [StatisticsStorageManager]. Deletes can only happen in a persistent fashion!
     *
     * @param column The [Name.ColumnName] to remove [ColumnMetrics] for.
     */
    fun deleteColumnStatistic(column: Name.ColumnName) {
        this.environment.executeInExclusiveTransaction { tx ->
            this.columnsStore.delete(tx, NameBinding.Column.objectToEntry(column)) // write to storage
        }
    }
}