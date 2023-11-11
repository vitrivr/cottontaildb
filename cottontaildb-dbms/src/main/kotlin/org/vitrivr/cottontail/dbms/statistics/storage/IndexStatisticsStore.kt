package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding

/**
 * A simple store for [IndexStatistic].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IndexStatisticsStore(private val store: Store) {
    /**
     * Retrieves a statistic [IndexStatistic] from the [IndexStatisticsStore].
     *
     * @param tx The [Transaction] to use.
     * @param index The [Name.IndexName] to retrieve [IndexStatistic] for.
     */
    operator fun get(tx: Transaction, index: Name.IndexName): IndexStatistic? {
       return this.store.get(tx, NameBinding.Index.toEntry(index))?.let { (IndexStatistic.deserialize(it)) }
    }

    /**
     * Updates the [IndexStatistic] for the [Name.IndexName].
     *
     * @param tx The [Transaction] to use.
     * @param index [Name.IndexName] to update [IndexStatistic] for.
     * @param statistic The new [IndexStatistic].
     * @return [IndexStatistic]
     */
    operator fun set(tx: Transaction, index: Name.IndexName, statistic: IndexStatistic) {
        this.store.put(tx, NameBinding.Index.toEntry(index), IndexStatistic.serialize(statistic))
    }

    /**
     * Deletes [IndexStatistic] for the given [Name.IndexName].
     *
     * @param tx The [Transaction] to use.
     * @param index [Name.IndexName] to delete.
     * @return True if [EntityMetric] was deleted, false otherwise.
     */
    fun delete(tx: Transaction, index: Name.IndexName) {
        this.store.delete(tx, NameBinding.Index.toEntry(index))
    }
}