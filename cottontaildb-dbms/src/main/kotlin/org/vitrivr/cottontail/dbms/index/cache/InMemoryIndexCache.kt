package org.vitrivr.cottontail.dbms.index.cache

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.lang.ref.SoftReference
import java.util.concurrent.locks.StampedLock

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class InMemoryIndexCache {
    /** */
    val cache = Object2ObjectLinkedOpenHashMap<Name.IndexName,MutableMap<LongRange,SoftReference<*>>>()

    /** Internal [StampedLock] to synchronise concurrent access. */
    val lock = StampedLock()

    /**
     * Tries to retrieve an element from the cache.
     *
     * @param key The [CacheKey] of the element to access.
     * @return The element or null.
     */
    inline fun <reified T> get(key: CacheKey): T? = this.lock.read {
        val dboRef = this.cache[key.name] ?: return@read null
        val rangeRef = dboRef[key.range] ?: return@read null
        val value = rangeRef.get()
        if (value is T) {
            value
        } else {
            dboRef.remove(key.range)
            null
        }
    }

    /**
     * Tries to store an element in the cache.
     *
     * @param key The [CacheKey] of the element to store.
     * @param value The value to store.
     * @return The element or null.
     */
    inline fun <reified T> put(key: CacheKey, value: T) = this.lock.write {
        this.cache.compute(key.name) { _, v ->
            val map = v ?: Object2ObjectLinkedOpenHashMap()
            map[key.range] = SoftReference(value)
            map
        }
    }

    /**
     * Invalidates an entry of this [InMemoryIndexCache].
     *
     * @param key The [CacheKey] of the element to invalidate.
     */
    fun invalidate(key: CacheKey) = this.lock.write {
        this.cache.compute(key.name) { _, v ->
            v?.remove(key.range)
            if (v.isNullOrEmpty()) {
                null
            } else {
                v
            }
        }
    }

    /**
     * Returns the size of this [InMemoryIndexCache].
     */
    fun size(): Int = this.cache.size
}