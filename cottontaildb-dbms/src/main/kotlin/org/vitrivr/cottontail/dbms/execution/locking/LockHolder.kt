package org.vitrivr.cottontail.dbms.execution.locking

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionMetadata

/**
 * Represents a holder of one or multiple [Lock]s.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
open class LockHolder<T>(val transactionId: TransactionId) : Comparable<LockHolder<*>> {
    /** The [Lock]s held by this [TransactionMetadata]. */
    private val locks = ObjectOpenHashSet<Lock<T>>()

    /** Returns the number of [Lock]s held by this [LockHolder]. */
    val numberOfLocks: Int
        get() = this.locks.size

    /**
     * Returns all [Lock]s held by this [LockHolder] as [List].
     */
    fun allLocks(): List<Lock<T>> = this.locks.toList()

    /**
     * Adds a [Lock] to the list of [Lock]s held by this [LockHolder].
     *
     * This is an internal function and should only be used by the [LockManager].
     *
     * @param lock The [Lock] that should be added.
     */
    internal fun addLock(lock: Lock<T>) {
        this.locks.add(lock)
    }

    /**
     * Removes a [Lock] from the list of [Lock]s held by this [LockHolder].
     *
     * This is an internal function and should only be used by the [LockManager].
     *
     * @param obj The [Lock] that should be removed.
     */
    internal fun removeLock(obj: Lock<T>) {
        this.locks.remove(obj)
    }

    /**
     * Compares this [LockHolder] to the other [LockHolder].
     */
    override operator fun compareTo(other: LockHolder<*>): Int = (this.transactionId - other.transactionId).toInt()

    /**
     * String representation of this [LockHolder].
     */
    override fun toString(): String = "tx${this.transactionId}"
}