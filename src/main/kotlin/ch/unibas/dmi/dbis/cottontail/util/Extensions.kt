package ch.unibas.dmi.dbis.cottontail.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock

fun ReentrantReadWriteLock.WriteLock.heldOrTry(timeout: Long, unit: TimeUnit): Boolean{
    return this.isHeldByCurrentThread || this.tryLock(timeout, unit)
}

inline fun <R> Lock.tryWith(action: () -> R): R {
    this.tryLock()
    val _return = action()
    this.unlock()
    return _return
}