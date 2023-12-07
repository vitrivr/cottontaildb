package org.vitrivr.cottontail.dbms.statistics.index

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A class that manages [IndexStatistic].
 *
 * The class basically acts as an in-memory buffer, so that [IndexStatistic] don't have to be
 * read and written from/to disk, every time they're updated.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IndexStatisticsManager(private val environment: Environment, transaction: Transaction) {

    companion object {

        /** The [Logger] used by this [IndexStatisticsManager]. */
        private val LOGGER = LoggerFactory.getLogger(IndexStatisticsManager::class.java)

        private const val INDEX_STATISTICS_STORE_NAME: String = "org.vitrivr.cottontail.statistics.index"
    }

    /** Internal [ConcurrentHashMap] of all statistics items. */
    private val statistics = Object2ObjectLinkedOpenHashMap<Name.IndexName,MutableMap<String,IndexStatistic>>()

    /** The Xodus [Store] backing this [IndexStatisticsManager]. */
    private val store: Store = this.environment.openStore(INDEX_STATISTICS_STORE_NAME, StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, transaction)

    /** Internal [ReentrantReadWriteLock] to synchronise concurrent access. */
    private val lock = ReentrantReadWriteLock()

    /** A flag indicating, that this [IndexStatisticsManager] has seen changes since the last time it was persisted. */
    @Volatile
    private var dirty: Boolean = false

    init {
        this.store.openCursor(transaction).use { cursor ->
            while (cursor.nextNoDup) {
                val name = NameBinding.Index.fromEntry(cursor.key)
                val map = Object2ObjectLinkedOpenHashMap<String,IndexStatistic>()
                do {
                    val item = IndexStatistic.entryToObject(cursor.value) as IndexStatistic
                    map[item.key] = item
                } while (cursor.nextDup)
                this.statistics[name] = map
            }
        }
    }

    /**
     * Retrieves all [IndexStatistic] for the given [Name.IndexName].
     *
     * @param index The [Name.IndexName] to retrieve the statistics for.
     * @return [Map] of of index statistics.
     */
    fun getAll(index: Name.IndexName): Map<String,IndexStatistic> = this.lock.read {
        this.statistics[index] ?: emptyMap()
    }


    /**
     * Retrieves a statistic [IndexStatistic] from the map.
     *
     * @param key The key to retrieve the statistics [IndexStatistic] for.
     */
    fun get(index: Name.IndexName, key: String): IndexStatistic? = this.lock.read {
        this.statistics[index]?.get(key)
    }

    /**
     * Updates a [IndexStatistic] in this [IndexStatisticsManager].
     *
     * @param index The key to update the statistics [IndexStatistic] for.
     * @param item The [IndexStatistic] to update
     */
    fun update(index: Name.IndexName, item: IndexStatistic) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics.compute(index) { _, v ->
            val map = v ?: Object2ObjectLinkedOpenHashMap()
            map[item.key] = item
            map
        }
    }

    /**
     * Updates a [IndexStatistic] in this [IndexStatisticsManager] in a persistent fashion.
     *
     * @param index The key to update the statistics [IndexStatistic] for.
     * @param item The [IndexStatistic] to update
     */
    fun updatePersistently(index: Name.IndexName, item: IndexStatistic, transaction: Transaction) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics.compute(index) { _, v ->
            val map = v ?: Object2ObjectLinkedOpenHashMap()
            map[item.key] = item
            map
        }
        this.store.put(transaction, NameBinding.Index.toEntry(index), IndexStatistic.objectToEntry(item))
        this.dirty = false
    }

    /**
     * Removes a [IndexStatistic] from this [IndexStatisticsManager]. Deletes can only happen in a persistent fashion!
     *
     * @param index The [Name.IndexName] to remove [IndexStatistic] for.
     */
    fun deletePersistently(index: Name.IndexName, transaction: Transaction) {
        this.dirty = true
        this.statistics.remove(index)
        this.store.delete(transaction, NameBinding.Index.toEntry(index))
        this.dirty = false
    }

    /**
     * Returns true, if this [IndexStatisticsManager] is dirty, i.e., has un-persisted changes.
     *
     * @return True if [IndexStatisticsManager] has un-persisted changes, false otherwise.
     */
    fun isDirty(): Boolean = this.dirty

    /**
     * Persists all the [IndexStatistic] items, if necessary.
     */
    fun persist() = this.lock.read {
        this.environment.executeInExclusiveTransaction {
            tx -> this.persistInTransaction(tx)
        }
    }

    /**
     * Persists all the [IndexStatistic] items, if necessary.
     *
     * @param transaction The [Transaction] to persist [IndexStatistic] in.
     */
    fun persistInTransaction(transaction: Transaction) = this.lock.read {
        for ((index, statistics) in this.statistics) {
            for (item in statistics.values) {
                this.store.put(transaction, NameBinding.Index.toEntry(index), IndexStatistic.objectToEntry(item))
            }
        }
        this.dirty = false
        LOGGER.debug("Index statistics persisted successfully!")
    }
}