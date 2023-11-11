package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding

/**
 * A class that manages the storage of [ColumnStatistic] by keeping them in memory and updating their value in the storage when necessary.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ColumnStatisticsStore(private val store: Store) {

    /**
     * Retrieves a statistic [ColumnStatistic] from the map.
     *
     * @param tx The [Transaction] to use.
     * @param column The key to retrieve the statistics [ColumnStatistic] for.
     */
    operator fun get(tx: Transaction, column: Name.ColumnName): ColumnStatistic? {
        return this.store.get(tx, NameBinding.Column.toEntry(column))?.let { (ColumnStatistic.entryToObject(it)) }
    }

    /**
     * Updates a statistic [ColumnStatistic] in this [ColumnStatisticsStore].
     *
     * @param tx The [Transaction] to use.
     * @param name The [Name.ColumnName] to set the [ColumnStatistic] for.
     * @param statistic The new [ColumnStatistic].
     */
    operator fun set(tx: Transaction, name: Name.ColumnName, statistic: ColumnStatistic) {
        this.store.put(tx, NameBinding.Column.toEntry(name), ColumnStatistic.objectToEntry(statistic)) // write to storage
    }

    /**
     * Removes a [ColumnStatistic] from this [ColumnStatisticsStore]. Deletes can only happen in a persistent fashion!
     *
     * @param tx The [Transaction] to use.
     * @param column The [Name.ColumnName] to remove [ColumnStatistic] for.
     */
    fun delete(tx: Transaction, column: Name.ColumnName) {
        this.store.delete(tx, NameBinding.Column.toEntry(column)) // write to storage
    }
}