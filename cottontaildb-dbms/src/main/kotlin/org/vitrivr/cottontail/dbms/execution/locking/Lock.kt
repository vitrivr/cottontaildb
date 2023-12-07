package org.vitrivr.cottontail.dbms.execution.locking

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.database.TransactionId
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write

/**
 * Represents a [Lock] on some object (usually a database object).
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class Lock<T> internal constructor(private val waitForGraph: WaitForGraph, val obj: T) {

    /** List of [LockHolder]s that hold a lock on this [Lock]. */
    private val owners = ObjectOpenHashSet<LockHolder<T>>()

    /** Internal [ReentrantLock] used to mediate access to this [Lock] object. */
    private val lock = ReentrantReadWriteLock(true)

    /** Internal [Condition] used to signal waiting threads that they can proceed. */
    private val waiters: Condition = this.lock.writeLock().newCondition()

    /** Number of shared locks held by this [Lock]. */
    private val sharedLockCount = AtomicInteger(0)

    /** Returns true, if this [Lock] is exclusively locked, false otherwise. */
    private val isExclusivelyLocked = AtomicBoolean(false)

    /** Returns true, if this [Lock] is shared locked, false otherwise. */
    private val isSharedLocked: Boolean
        get() = this.sharedLockCount.get() > 0

    /**
     * Acquires a hold on this [Lock].
     *
     * @param txn [LockHolder] that tries to acquire the lock.
     * @param requestedLockMode The [LockMode] the transaction tries to acquire.
     */
    fun acquire(txn: LockHolder<T>, requestedLockMode: LockMode) = this.lock.write {
        if (this.owners.contains(txn)) {
            when (requestedLockMode) {
                LockMode.EXCLUSIVE -> upgradeSharedLock(txn)
                else -> { /* Lock already held; no action needed. */ }
            }
        } else {
            when (requestedLockMode) {
                LockMode.SHARED -> acquireSharedLock(txn)
                LockMode.EXCLUSIVE -> acquireExclusiveLock(txn)
                else -> throw IllegalArgumentException("Lock mode of type $requestedLockMode cannot be explicitly acquired!")
            }
        }
    }

    /**
     * Releases a hold on this [Lock].
     *
     * @param txn [LockHolder] that tries to release the lock.
     */
    fun release(txn: LockHolder<T>) = this.lock.write {
        if (this.owners.remove(txn)) {
            this.waitForGraph.remove(txn)
            this.sharedLockCount.decrementAndGet()
            this.isExclusivelyLocked.compareAndExchange(true, false)
            txn.removeLock(this)
            this.waiters.signalAll()
        }
    }

    /**
     * Returns an unmodifiable set of all [TransactionId]s that currently have a hold on this [Lock]
     */
    fun getOwners(): Set<LockHolder<T>> = Collections.unmodifiableSet(this.owners)

    /**
     * Returns the [LockMode] for this [Lock].
     *
     * @return The [LockManager] for this [Lock].
     */
    fun getMode(): LockMode = when {
        this.isExclusivelyLocked.get() -> LockMode.EXCLUSIVE
        this.isSharedLocked -> LockMode.SHARED
        else -> LockMode.NO_LOCK
    }

    /**
     * Tries to upgrade the hold on this [Lock].
     *
     * @param txn [LockHolder] that tries to upgrade the [Lock].
     */
    private fun upgradeSharedLock(txn: LockHolder<T>) {
        if (this.isExclusivelyLocked.get()) return
        while (this.sharedLockCount.get() > 1) {
            val ownersWithSelfRemoved: Set<LockHolder<T>> = ObjectOpenHashSet(this.owners.filter { it != txn })
            this.waitForGraph.add(txn, ownersWithSelfRemoved) /* Ok to add on each iteration, because WaitForGraph takes care of de-duplication. */
            this.waitForGraph.detectDeadlock(txn)
            this.waiters.await()
        }
        check(this.sharedLockCount.compareAndSet(1, 0)) { "Transaction ${txn.transactionId} failed to upgrade lock $this: expected one shared lock (own)." }
        check(this.isExclusivelyLocked.compareAndSet(false, true)) { "Transaction ${txn.transactionId} failed to upgrade lock $this: non-exclusive lock." }
    }

    /**
     * Tries to acquire a shared lock on this [Lock]. Blocks until the lock could be acquired,
     *
     * @param txn [LockHolder] that tries to acquire the lock.
     */
    private fun acquireSharedLock(txn: LockHolder<T>) {
        while (this.isExclusivelyLocked.get() || this.lock.hasWaiters(waiters)) {
            this.waitForGraph.add(txn, this.owners) /* Ok to add on each iteration, because WaitForGraph takes care of de-duplication. */
            this.waitForGraph.detectDeadlock(txn)
            this.waiters.await()
        }
        this.sharedLockCount.incrementAndGet()
        this.owners.add(txn)
        txn.addLock(this)
    }

    /**
     * Tries to acquire an exclusive lock on this [Lock]. Blocks until the lock could be acquired,
     *
     * @param txn [LockHolder] that tries to acquire the exclusive lock.
     */
    private fun acquireExclusiveLock(txn: LockHolder<T>) {
        if (this.isExclusivelyLocked.get() || this.isSharedLocked)
        while (this.isExclusivelyLocked.get() || this.isSharedLocked) {
            this.waitForGraph.add(txn, this.owners) /* Ok to add on each iteration, because WaitForGraph takes care of de-duplication. */
            this.waitForGraph.detectDeadlock(txn)
            this.waiters.await()
        }
        check(this.isExclusivelyLocked.compareAndSet(false, true)) { "Transaction ${txn.transactionId} failed to acquire lock $this: Lock is being held exclusively." }
        this.owners.add(txn)
        txn.addLock(this)
    }
}