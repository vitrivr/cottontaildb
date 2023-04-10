package org.vitrivr.cottontail.dbms.statistics.storage

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A class that manages  the storage of [ColumnMetrics] by keeping them in memory and updating their value in the storage when necessary.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */

class StatisticsStorageManager(private val environment: Environment, transaction: Transaction) {

    /** Internal [Object2ObjectLinkedOpenHashMap] of all statistics items. */
    private val statistics = Object2ObjectLinkedOpenHashMap<Name.ColumnName, ColumnMetrics>()

    /** The Xodus [Store] backing this [StatisticsStorageManager]. */
    private val store: Store = this.environment.openStore("StatisticsStorageManager", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

    /** Internal [ReentrantReadWriteLock] to synchronise concurrent access. */
    private val lock = ReentrantReadWriteLock()

    init{
        /* Reads all entries once. */
        this.store.openCursor(transaction).use { cursor ->
            while (cursor.next) {
                val name = NameBinding.Column.entryToObject(cursor.key) as Name.ColumnName
                this.statistics[name] = (ColumnMetrics.Serialized.entryToObject(cursor.value) as ColumnMetrics.Serialized).toActual(name)
            }
        }
    }

    /**
     * Retrieves a statistic [ColumnMetrics] from the map.
     *
     * @param column The key to retrieve the statistics [ColumnMetrics] for.
     */
    operator fun get(column: Name.ColumnName): ColumnMetrics? = this.lock.read {
        this.statistics[column]
    }

    /**
     * Updates a statistic [ColumnMetrics] in this [StatisticsStorageManager].
     *
     * @param statistic The new [ColumnMetrics].
     * @param transaction The [Transaction] to perform the update with.
     */
    fun updatePersistently(statistic: ColumnMetrics, transaction: Transaction) = this.lock.write {
        this.statistics[statistic.name] = statistic // keep in memory
        this.store.put(transaction, NameBinding.Column.objectToEntry(statistic.name), ColumnMetrics.Serialized.objectToEntry(statistic.toSerialized())) // write to storage
    }

    /**
     * Removes a statistic [ColumnMetrics] from this [StatisticsStorageManager]. Deletes can only happen in a persistent fashion!
     *
     * @param column The [Name.ColumnName] to remove [ColumnMetrics] for.
     */
    fun deletePersistently(column: Name.ColumnName, transaction: Transaction) {
        this.statistics.remove(column)
        this.store.delete(transaction, NameBinding.Column.objectToEntry(column))
    }

}