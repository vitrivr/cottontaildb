package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import java.io.Closeable

/**
 * A class that manages  the storage of [ColumnStatistic] by keeping them in memory and updating their value in the storage when necessary.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.2.0
 */
class StatisticsStorageManager(config: Config): Closeable {

    /** The statistics/metrics Xodus [Environment] used by the [StatisticsStorageManager]. */
    private val environment: Environment = Environments.newInstance(config.statisticsFolder().toFile(), config.xodus.toEnvironmentConfig())

    /** The Xodus [Store] backing this [StatisticsStorageManager]. */
    private val columnsStore: Store

    /** The Xodus [Store] backing this [StatisticsStorageManager]. */
    private val metricsStore: Store

    /** The Xodus [Store] backing this [StatisticsStorageManager]. */
    private val indexStore: Store

    init{
        val tx = this.environment.beginExclusiveTransaction()
        try {
            this.columnsStore = this.environment.openStore("org.vitrivr.cottontail.statistics.column", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx)
            this.metricsStore = this.environment.openStore("org.vitrivr.cottontail.statistics.entity", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx)
            this.indexStore = this.environment.openStore("org.vitrivr.cottontail.statistics.index", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, tx)
        } finally {
            tx.commit()
        }
    }

    /**
     * Retrieves a statistic [ColumnStatistic] from the map.
     *
     * @param column The [Name.ColumnName] to retrieve the statistics [ColumnStatistic] for.
     * @return [ColumnStatistic] or null if no [ColumnStatistic] exists for the given [Name.ColumnName].
     */
    operator fun get(column: Name.ColumnName): ColumnStatistic? = this.environment.computeInReadonlyTransaction { tx ->
        this.columnsStore.get(tx, NameBinding.Column.toEntry(column))?.let { (ColumnStatistic.entryToObject(it)) }
    }

    /**
     * Updates a statistic [ColumnStatistic] in this [StatisticsStorageManager].
     *
     * @param name The [Name.ColumnName] to set the [ColumnStatistic] for.
     * @param statistic The new [ColumnStatistic].
     */
    operator fun set(name: Name.ColumnName, statistic: ColumnStatistic) {
        this.environment.executeInExclusiveTransaction { tx ->
            this.columnsStore.put(tx, NameBinding.Column.toEntry(name), ColumnStatistic.objectToEntry(statistic)) // write to storage
        }
    }

    /**
     * Retrieves a statistic [ColumnStatistic] from the map.
     *
     * @param column The [Name.ColumnName] to retrieve the statistics [ColumnStatistic] for.
     * @return [IndexStatistic] or null if no [IndexStatistic] exists for the given [Name.IndexName].
     */
    operator fun get(column: Name.IndexName): List<IndexStatistic> = this.environment.computeInReadonlyTransaction { tx ->
        this.indexStore.openCursor(tx).use { cursor ->
            val result = mutableListOf<IndexStatistic>()
            cursor.getSearchKey(NameBinding.Index.toEntry(column)) ?: return@computeInReadonlyTransaction result
            do {
                result.add(IndexStatistic.entryToObject(cursor.value))
            } while (cursor.nextDup)
            result
        }
    }

    /**
     * Updates a statistic [IndexStatistic] in this [StatisticsStorageManager].
     *
     * @param name The [Name.IndexName] to set the [IndexStatistic] for.
     * @param statistic The new [IndexStatistic].
     */
    operator fun set(name: Name.IndexName, statistic: List<IndexStatistic>) {
        this.environment.executeInExclusiveTransaction { tx ->
            val key = NameBinding.Index.toEntry(name)
            this.indexStore.delete(tx, NameBinding.Index.toEntry(name)) // write to storage
            for (s in statistic) {
                this.indexStore.put(tx, key, IndexStatistic.objectToEntry(s)) // write to storage
            }
        }
    }

    /**
     * Reads and returns an [EntityMetric] for the given [Name.EntityName].
     *
     * @param entity [Name.EntityName] to read [EntityMetric] for.
     * @return [EntityMetric]
     */
    fun getMetric(entity: Name.EntityName): EntityMetric? = this.environment.computeInReadonlyTransaction { tx ->
        this.metricsStore.get(tx, NameBinding.Entity.toEntry(entity))?.let { EntityMetric.entryToObject(it) }
    }

    /**
     * Updates the [EntityMetric] for the [Name.EntityName].
     *
     * @param entity [Name.EntityName] to update [EntityMetric] for.
     * @param metric The new [EntityMetric].
     * @return [EntityMetric]
     */
    fun setMetric(entity: Name.EntityName, metric: EntityMetric): Boolean = this.environment.computeInExclusiveTransaction { tx ->
        this.metricsStore.put(tx, NameBinding.Entity.toEntry(entity), EntityMetric.objectToEntry(metric))
    }

    /**
     * Deletes [EntityMetric] for the given [Name.EntityName].
     *
     * @param entity [Name.EntityName] to delete [EntityMetric] for.
     * @return True if [EntityMetric] was deleted, false otherwise.
     */
    fun deleteMetric(entity: Name.EntityName): Boolean = this.environment.computeInExclusiveTransaction { tx ->
        this.metricsStore.delete(tx, NameBinding.Entity.toEntry(entity))
    }

    /**
     * Removes a [ColumnStatistic] from this [StatisticsStorageManager]. Deletes can only happen in a persistent fashion!
     *
     * @param column The [Name.ColumnName] to remove [ColumnStatistic] for.
     */
    fun deleteColumnStatistic(column: Name.ColumnName) {
        this.environment.executeInExclusiveTransaction { tx ->
            this.columnsStore.delete(tx, NameBinding.Column.toEntry(column)) // write to storage
        }
    }

    /**
     * Removes a [IndexStatistic] from this [StatisticsStorageManager]. Deletes can only happen in a persistent fashion!
     *
     * @param name The [Name.IndexName] to remove [IndexStatistic] for.
     */
    fun deleteIndexStatistic(name: Name.IndexName) {
        this.environment.executeInExclusiveTransaction { tx ->
            this.indexStore.delete(tx,  NameBinding.Index.toEntry(name)) // write to storage
        }
    }

    /**
     * Closes the [Environment] backing this [StatisticsStorageManager].
     */
    override fun close() {
        this.environment.close()
    }
}