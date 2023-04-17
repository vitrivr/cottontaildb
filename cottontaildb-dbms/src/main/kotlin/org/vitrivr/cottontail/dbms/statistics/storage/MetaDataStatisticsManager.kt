package org.vitrivr.cottontail.dbms.statistics.storage

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.bindings.IntegerBinding
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

class MetaDataStatisticsManager(private val environment: Environment, transaction: Transaction) {

    /** Internal [Object2ObjectLinkedOpenHashMap] representation. */
    private val changes = Object2ObjectLinkedOpenHashMap<Name.EntityName, Int>()

    /** The Xodus [Store] backing this [MetaDataStatisticsManager]. */
    private val store: Store = this.environment.openStore("MetaDataStatisticsManager", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

    /** Internal [ReentrantReadWriteLock] to synchronise concurrent access. */
    private val lock = ReentrantReadWriteLock()

    init{
        /* Reads all entries once. */
        this.store.openCursor(transaction).use { cursor ->
            while (cursor.next) {
                val name = NameBinding.Entity.entryToObject(cursor.key) as Name.EntityName
                this.changes[name] = IntegerBinding.entryToInt(cursor.value)
            }
        }
    }

    /**
     * Retrieves the number of changes from the map.
     *
     * @param entity The key to retrieve the metadata for.
     */
    operator fun get(entity: Name.EntityName): Int = this.lock.read {
        val changes = this.changes[entity]
        if (changes != null) {
            return changes
        } else {
            return 0
        }
    }

    /**
     * Increases the number of changes for the [Name.EntityName] in this [MetaDataStatisticsManager].
     *
     * @param entity The [Name.EntityName] to update.
     * @param transaction The [Transaction] to perform the update with.
     */
    fun increateEntityChanges(entity: Name.EntityName, transaction: Transaction) = this.lock.write {
        // either way, increase count of changes by 1.
        this.changes[entity]?.let {
            // if not null
            val changes = it + 1
            this.changes[entity] = changes
            this.store.put(transaction, NameBinding.Entity.objectToEntry(entity), IntegerBinding.intToEntry(changes)) // write to storage
        } ?: {
            // if null
            val changes = 1
            this.changes[entity] = changes
            this.store.put(transaction, NameBinding.Entity.objectToEntry(entity), IntegerBinding.intToEntry(changes)) // write to storage
        }
    }

    /**
     * Increases the number of changes for the [Name.EntityName] in this [MetaDataStatisticsManager].
     *
     * @param entity The [Name.EntityName] to update.
     * @param transaction The [Transaction] to perform the update with.
     */
    fun resetEntityChanges(entity: Name.EntityName, transaction: Transaction) = this.lock.write {
        // set entity changes to 0
        this.store.put(transaction, NameBinding.Entity.objectToEntry(entity), IntegerBinding.intToEntry(0))
    }

    /**
     * Removes a statistic [ColumnMetrics] from this [StatisticsStorageManager]. Deletes can only happen in a persistent fashion!
     *
     * @param column The [Name.ColumnName] to remove [ColumnMetrics] for.
     */
    fun deleteEntity(entity: Name.EntityName, transaction: Transaction) {
        this.changes.remove(entity)
        this.store.delete(transaction, NameBinding.Entity.objectToEntry(entity))
    }

}