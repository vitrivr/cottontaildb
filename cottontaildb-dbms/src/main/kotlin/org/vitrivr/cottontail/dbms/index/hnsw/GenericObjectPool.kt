package org.vitrivr.cottontail.dbms.index.hnsw

import java.io.Serializable
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

/**
 * Generic object pool.
 *
 * @param <T> type of object to pool
</T> */
class GenericObjectPool<T>(supplier: Supplier<T>, maxPoolSize: Int) : Serializable {
    private val items: ArrayBlockingQueue<T>

    /**
     * Constructs a new pool
     *
     * @param supplier used to create instances of the object to pool
     * @param maxPoolSize maximum items to have in the pool
     */
    init {
        items = ArrayBlockingQueue(maxPoolSize)
        for (i in 0 until maxPoolSize) {
            items.add(supplier.get())
        }
    }

    /**
     * Borrows an object from the pool.
     *
     * @return the borrowed object
     */
    fun borrowObject(): T {
        return try {
            items.take()
        } catch (e: InterruptedException) {
            throw RuntimeException(e) // TODO jk any more elegant way to do this ?
        }
    }

    /**
     * Returns an instance to the pool. By contract, obj must have been obtained using [GenericObjectPool.borrowObject]
     *
     * @param item the item to return to the pool
     */
    fun returnObject(item: T) {
        items.add(item)
    }

    companion object {
        private const val serialVersionUID = 1L
    }
}