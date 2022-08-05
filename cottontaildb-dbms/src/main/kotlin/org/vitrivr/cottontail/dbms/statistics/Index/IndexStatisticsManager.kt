package org.vitrivr.cottontail.dbms.statistics.Index

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
    var dirty: Boolean = false
        private set

    init {
        this.store.openCursor(transaction).use { cursor ->
            while (cursor.nextNoDup) {
                val name = NameBinding.Index.entryToObject(cursor.key) as Name.IndexName
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
     * Updates a statistic [IndexStatistic] in this [IndexStatisticsManager].
     *
     * @param index The key to update the statistics [IndexStatistic] for.
     * @param item The [IndexStatistic] to update
     */
    fun update(index: Name.IndexName, item: IndexStatistic) = this.lock.write {
        this.dirty = true /* Update dirty flag. */
        this.statistics.compute(index) { k, v ->
            val map = v ?: Object2ObjectLinkedOpenHashMap()
            map[item.key] = item
            map
        }
    }

    /**
     * Persists all the [IndexStatistic] items, if necessary.
     *
     * @return True if content of this [IndexStatisticsManager] was persisted, false otherwise.
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
     * @return True if content of this [IndexStatisticsManager] was persisted, false otherwise.
     */
    fun persistInTransaction(transaction: Transaction): Boolean = this.lock.read {
        if (this.dirty) {
            for ((index, statistics) in this.statistics) {
                for (item in statistics.values) {
                    this.store.put(transaction, NameBinding.Index.objectToEntry(index), IndexStatistic.objectToEntry(item))
                }
            }
            this.dirty = false
            LOGGER.debug("Index statistics persisted successfully!")
            true
        } else {
            false
        }
    }
}