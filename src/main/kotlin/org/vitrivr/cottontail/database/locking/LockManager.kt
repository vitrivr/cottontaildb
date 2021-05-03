package org.vitrivr.cottontail.database.locking

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock

/**
 * A [LockManager] implementation that allows for management of [Lock]s on [DBO]s.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LockManager<T> {

    /** List of all [Lock]s managed by this [LockManager]. */
    private val locks = Object2ObjectLinkedOpenHashMap<T, Lock<T>>()

    /** The [WaitForGraph] data structure used to detect deadlock situations. */
    private val waitForGraph: WaitForGraph = WaitForGraph()

    /** [StampedLock] used to mediate access to [LockManager]. */
    private val accessLock = StampedLock()

    /**
     * Returns an list all [T] that are currently locked.
     *
     * @return List of all [T]s that are currently locked.
     */
    fun allLocks(): List<Pair<T, Lock<T>>> = this.accessLock.read {
        this.locks.map { v -> v.key to v.value }
    }

    /**
     * Tries to acquire a lock on [Name] for the given [LockHolder].
     *
     * @param txn [LockHolder] to acquire the lock for.
     * @param obj Object [T] to acquire a lock on.
     * @param mode The [LockMode]
     */
    fun lock(txn: LockHolder<T>, obj: T, mode: LockMode) = this.accessLock.write {
        require(mode != LockMode.NO_LOCK) { "Cannot acquire a lock of mode $mode; try LockManager.release()." }
        val lockOn = txn.lockOn(obj)
        if (lockOn === mode) {
            return
        } else if (lockOn === LockMode.EXCLUSIVE && mode === LockMode.SHARED) {
            return
        } else if (lockOn === LockMode.SHARED && mode === LockMode.EXCLUSIVE) {
            val lock: Lock<T> = this.locks.computeIfAbsent(obj) { Lock(this.waitForGraph) }
            lock.upgrade(txn)
            txn.addLock(obj, lock)
        } else {
            val lock: Lock<T> = this.locks.computeIfAbsent(obj) { Lock(this.waitForGraph) }
            lock.acquire(txn, mode)
            txn.addLock(obj, lock)
        }
    }

    /**
     * Unlocks the lock on [Name] held by the given [LockHolder].
     *
     * @param txn [LockHolder] to release the lock for.
     * @param obj Object [T] to release the lock on.
     */
    fun unlock(txn: LockHolder<T>, obj: T) = this.accessLock.write {
        this.locks.computeIfPresent(obj) { _, lock ->
            lock.release(txn)
            txn.removeLock(obj)
            if (lock.getMode() === LockMode.NO_LOCK) {
                null
            } else {
                lock
            }
        }
    }
}