package org.vitrivr.cottontail.database.locking

import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.model.basics.TransactionId

/**
 * Represents a holder of one or multiple [Lock]s.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
open class LockHolder<T>(val txId: TransactionId) : Comparable<LockHolder<*>> {
    /** The [Lock]s held by this [Transaction]. */
    protected val locks = Object2ObjectMaps.synchronize(Object2ObjectOpenHashMap<T, Lock<T>>())

    /** Returns the number of [Lock]s held by this [LockHolder]. */
    val numberOfLocks: Int
        get() = this.locks.size


    /**
     * Adds a [Lock] to the list of [Lock]s held by this [LockHolder].
     *
     * This is an internal function and should only be used by the [LockManager].
     *
     * @param lock The [Lock] that should be added.
     */
    internal fun addLock(obj: T, lock: Lock<T>) {
        this.locks[obj] = lock
    }

    /**
     * Removes a [Lock] from the list of [Lock]s held by this [LockHolder].
     *
     * This is an internal function and should only be used by the [LockManager].
     *
     * @param obj The [Lock] that should be removed.
     */
    internal fun removeLock(obj: T) {
        this.locks.remove(obj)
    }

    /**
     * Returns the [LockMode] the given [LockHolder] has on the given obj [T]. If it holds
     * no lock, then [LockMode.NO_LOCK] is returned.
     *
     * @param obj The object [T] to check.
     * @return [LockMode]
     */
    fun lockOn(obj: T): LockMode = this.locks[obj]?.getMode() ?: LockMode.NO_LOCK

    /**
     * Compares this [LockHolder] to the other [LockHolder].
     */
    override operator fun compareTo(other: LockHolder<*>): Int = (this.txId - other.txId).toInt()
}