package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding

/**
 * A class that manages the persistent storage of [EntityMetric]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityMetricsStore(private val store: Store) {
    /**
     * Retrieves a statistic [ColumnStatistic] from the map.
     *
     * @param tx The [Transaction] to use.
     * @param entity The key to retrieve the statistics [ColumnStatistic] for.
     */
    operator fun get(tx: Transaction, entity: Name.EntityName): EntityMetric? {
       return this.store.get(tx, NameBinding.Entity.toEntry(entity))?.let { (EntityMetric.entryToObject(it)) }
    }

    /**
     * Updates a statistic [ColumnStatistic] in this [EntityMetric].
     *
     * @param tx The [Transaction] to use.
     * @param entity The [Name.EntityName] to set the [EntityMetric] for.
     * @param statistic The new [EntityMetric].
     */
    operator fun set(tx: Transaction, entity: Name.EntityName, statistic: EntityMetric) {
        this.store.put(tx, NameBinding.Entity.toEntry(entity), EntityMetric.objectToEntry(statistic)) // write to storage
    }

    /**
     * Removes a [EntityMetric] from this [EntityMetricsStore]. Deletes can only happen in a persistent fashion!
     *
     * @param tx: Transaction
     * @param entity The [Name.EntityName] to remove [EntityMetric] for.
     */
    fun delete(tx: Transaction, entity: Name.EntityName) {
        this.store.delete(tx, NameBinding.Entity.toEntry(entity)) // write to storage
    }
}