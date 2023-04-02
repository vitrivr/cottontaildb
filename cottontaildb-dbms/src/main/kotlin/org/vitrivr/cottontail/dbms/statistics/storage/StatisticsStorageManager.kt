package org.vitrivr.cottontail.dbms.statistics.storage

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.statistics.index.IndexStatistic
import org.vitrivr.cottontail.dbms.statistics.index.IndexStatisticsManager
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

    /** A flag indicating, that this [StatisticsStorageManager] has seen changes since the last time it was persisted. */
    @Volatile
    private var dirty: Boolean = false

    init{
        TODO("Do we actually need to read all entries at once and keep them in memory? No right?")
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
     * @param statistic The [ColumnMetrics] to update.
     */
    fun update(statistic: ColumnMetrics) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics[statistic.name] = statistic
    }

    /**
     * Updates a statistic [ColumnMetrics] in this [StatisticsStorageManager].
     *
     * @param statistic The new [ColumnMetrics].
     * @param transaction The [Transaction] to perform the update with.
     */
    fun updatePersistently(statistic: ColumnMetrics, transaction: Transaction) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics[statistic.name] = statistic
        this.store.put(transaction, NameBinding.Column.objectToEntry(statistic.name), ColumnMetrics.Serialized.objectToEntry(statistic.toSerialized()))
        this.dirty = false
    }

    /**
     * Removes a statistic [ColumnMetrics] from this [StatisticsStorageManager]. Deletes can only happen in a persistent fashion!
     *
     * @param column The [Name.ColumnName] to remove [ColumnMetrics] for.
     */
    fun deletePersistently(column: Name.ColumnName, transaction: Transaction) {
        this.dirty = true
        this.statistics.remove(column)
        this.store.delete(transaction, NameBinding.Column.objectToEntry(column))
        this.dirty = false
    }

    /**
     * Returns true, if this [StatisticsStorageManager] is dirty, i.e., has un-persisted changes.
     *
     * @return True if [StatisticsStorageManager] has un-persisted changes, false otherwise.
     */
    fun isDirty(): Boolean
            = this.dirty

    /**
     * Persists all the [IndexStatistic] items, if necessary.
     *
     * @return True if content of this [IndexStatisticsManager] was persisted, false otherwise.
     */
    fun persist() = this.lock.read {
        this.environment.executeInExclusiveTransaction { tx -> this.persistInTransaction(tx) }
    }

    /**
     * Persists all the [IndexStatistic] items, if necessary.
     *
     * @param transaction The [Transaction] to persist [IndexStatistic] in.
     * @return True if content of this [IndexStatisticsManager] was persisted, false otherwise.
     */
    fun persistInTransaction(transaction: Transaction): Unit = this.lock.read {
        for ((column, statistic) in this.statistics) {
            this.store.put(transaction, NameBinding.Column.objectToEntry(column), ColumnMetrics.Serialized.objectToEntry(statistic.toSerialized()))
        }
        this.dirty = false
    }
}