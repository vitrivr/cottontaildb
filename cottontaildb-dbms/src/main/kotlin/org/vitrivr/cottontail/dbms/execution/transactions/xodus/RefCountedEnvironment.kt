package org.vitrivr.cottontail.dbms.execution.transactions.xodus

import jetbrains.exodus.env.Environment
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.StampedLock

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RefCountedEnvironment(val environment: Environment): Environment by environment {
    private val reference = AtomicInteger(0)

    /** A [StampedLock] that mediates */
    private val lock = StampedLock()

    /**
     * Returns the number of [Tx]s that reference this [RefCountedEnvironment].
     *
     * @return Number of [Tx]s that reference this [RefCountedEnvironment].
     */
    fun references(): Int = this.reference.get()

    /**
     *
     */
    inner class Tx(val parent: Transaction) {

        /** The (optional) stamp held by this [Tx] */
        private val stamp: Long?

        /** */
        val environment: RefCountedEnvironment = this@RefCountedEnvironment

        init {
           if (!this.parent.type.readonly) {
               this.stamp = this@RefCountedEnvironment.lock.writeLock()
           } else {
               this.stamp = null
           }
            this@RefCountedEnvironment.reference.incrementAndGet()
        }

        /** The Xodus [jetbrains.exodus.env.Transaction] backing this [Tx]. */
        internal val xodusTx = if (this.parent.type.readonly) {
            this@RefCountedEnvironment.environment.beginReadonlyTransaction()
        } else {
            this@RefCountedEnvironment.environment.beginExclusiveTransaction()
        }

        /**
         * Commits this [Tx].
         */
        fun commit(): Boolean {
            try {
                return this.xodusTx.commit()
            } finally {
                this@RefCountedEnvironment.reference.decrementAndGet()
                if (this.stamp != null) {
                    this@RefCountedEnvironment.lock.unlock(this.stamp)
                }
            }
        }

        /**
         * Aborts this [Tx].
         */
        fun abort() {
            try {
                this.parent.xodusTx.abort()
            } finally {
                this@RefCountedEnvironment.reference.decrementAndGet()
                if (this.stamp != null) {
                    this@RefCountedEnvironment.lock.unlock(this.stamp)
                }
            }
        }
    }
}