package org.vitrivr.cottontail.dbms.execution.transactions

import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.ExecutionContext
import org.vitrivr.cottontail.dbms.execution.locking.Lock
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.locking.LockMode
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx

/**
 * A [TransactionContext] can be used to query and interact with a [Transaction].
 *
 * This is the view of a [Transaction] that is available to the operators that execute a query.
 *
 * @author Ralph Gasser
 * @version 1.5.1
 */
interface TransactionContext: ExecutionContext {

    /** The [TransactionId] of this [TransactionContext]. */
    val txId: TransactionId

    /** The Xodus [Transaction] associated with this [TransactionContext]. */
    val xodusTx: Transaction

    /**
     * Caches a [Tx] for later re-use.
     *
     * @param tx The [DBO] to create the [Tx] for.
     * @return True on success, false otherwise.
     */
    fun cacheTx(tx: Tx): Boolean

    /**
     * Obtains a cached [Tx] for the given [DBO].
     *
     * @param dbo The [DBO] to create the [Tx] for.
     * @return The resulting [Tx] or null
     */
    fun <T: Tx> getCachedTxForDBO(dbo: DBO): T?

    /**
     * Acquires a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
     * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
     *
     * @param dbo [DBO] The [DBO] to request the lock for.
     * @param mode The desired [LockMode]
     */
    fun requestLock(dbo: DBO, mode: LockMode)

    /**
     * Signals an [Event] to this [TransactionContext].
     *
     * This method is a facility to communicate actions that take place within a
     * [TransactionContext] to the 'outside' world. Usually, that communication
     * must be withheld until the [TransactionContext] commits.
     *
     * @param event The [Event] that has been reported.
     */
    fun signalEvent(event: Event)
}