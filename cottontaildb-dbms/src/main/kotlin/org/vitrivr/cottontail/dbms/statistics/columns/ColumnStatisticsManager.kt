package org.vitrivr.cottontail.dbms.statistics.columns

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
 * A class that manages [ColumnStatistic].
 *
 * The class basically acts as an in-memory buffer, so that [ColumnStatistic] don't have to be
 * read and written from/to disk every time they're updated or accessed.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ColumnStatisticsManager(private val environment: Environment, transaction: Transaction) {

    companion object {
        private const val CATALOGUE_STATISTICS_STORE_NAME: String = "ctt_cat_statistics"
    }

    /** Internal [Object2ObjectLinkedOpenHashMap] of all statistics items. */
    private val statistics = Object2ObjectLinkedOpenHashMap<Name.ColumnName,ColumnStatistic>()

    /** The Xodus [Store] backing this [ColumnStatisticsManager]. */
    private val store: Store = this.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

    /** Internal [ReentrantReadWriteLock] to synchronise concurrent access. */
    private val lock = ReentrantReadWriteLock()

    /** A flag indicating, that this [ColumnStatisticsManager] has seen changes since the last time it was persisted. */
    @Volatile
    private var dirty: Boolean = false

    init {
        /* Reads all entries once. */
        this.store.openCursor(transaction).use { cursor ->
            while (cursor.next) {
                val name = NameBinding.Column.entryToObject(cursor.key) as Name.ColumnName
                this.statistics[name] = (ColumnStatistic.Serialized.entryToObject(cursor.value) as ColumnStatistic.Serialized).toActual(name)
            }
        }
    }

    /**
     * Retrieves a statistic [IndexStatistic] from the map.
     *
     * @param column The key to retrieve the statistics [IndexStatistic] for.
     */
    operator fun get(column: Name.ColumnName): ColumnStatistic? = this.lock.read {
        this.statistics[column]
    }

    /**
     * Updates a statistic [ColumnStatistic] in this [ColumnStatisticsManager].
     *
     * @param statistic The [ColumnStatistic] to update.
     */
    fun update(statistic: ColumnStatistic) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics[statistic.name] = statistic
    }

    /**
     * Updates a statistic [ColumnStatistic] in this [ColumnStatisticsManager].
     *
     * @param statistic The new [ColumnStatistic].
     * @param transaction The [Transaction] to perform the update with.
     */
    fun updatePersistently(statistic: ColumnStatistic, transaction: Transaction) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics[statistic.name] = statistic
        this.store.put(transaction, NameBinding.Column.objectToEntry(statistic.name), ColumnStatistic.Serialized.objectToEntry(statistic.toSerialized()))
        this.dirty = false
    }

    /**
     * Removes a statistic [ColumnStatistic] from this [ColumnStatisticsManager]. Deletes can only happen in a persistent fashion!
     *
     * @param column The [Name.ColumnName] to remove [ColumnStatistic] for.
     */
    fun deletePersistently(column: Name.ColumnName, transaction: Transaction) {
        this.dirty = true
        this.statistics.remove(column)
        this.store.delete(transaction, NameBinding.Column.objectToEntry(column))
        this.dirty = false
    }

    /**
     * Returns true, if this [ColumnStatisticsManager] is dirty, i.e., has un-persisted changes.
     *
     * @return True if [ColumnStatisticsManager] has un-persisted changes, false otherwise.
     */
    fun isDirty(): Boolean
        = this.dirty || (!this.statistics.all { it.value.statistics.fresh })

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
            this.store.put(transaction, NameBinding.Column.objectToEntry(column), ColumnStatistic.Serialized.objectToEntry(statistic.toSerialized()))
        }
        this.dirty = false
    }
}