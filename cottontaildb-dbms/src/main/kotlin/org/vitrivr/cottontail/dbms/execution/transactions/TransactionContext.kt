package org.vitrivr.cottontail.dbms.execution.transactions

import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.execution.ExecutionContext
import org.vitrivr.cottontail.dbms.execution.locking.Lock
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.locking.LockMode
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.operations.Operation

/**
 * A [TransactionContext] can be used to query and interact with a [Transaction].
 *
 * This is the view of a [Transaction] that is available to the operators that execute a query.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
interface TransactionContext: ExecutionContext {

    /** The [TransactionId] of this [TransactionContext]. */
    val txId: TransactionId

    /** The Xodus [Transaction] associated with this [TransactionContext]. */
    val xodusTx: Transaction

    /**
     * Obtains a [Tx] for the given [DBO]. This method should make sure, that only one [Tx] per [DBO] is created.
     *
     * @param dbo The DBO] to create the [Tx] for.
     * @return The resulting [Tx]
     */
    fun getTx(dbo: DBO): Tx

    /**
     * Acquires a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
     * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
     *
     * @param dbo [DBO] The [DBO] to request the lock for.
     * @param mode The desired [LockMode]
     */
    fun requestLock(dbo: DBO, mode: LockMode)

    /**
     * Signals a [Operation.DataManagementOperation] to this [TransactionContext].
     *
     * Implementing methods must process these [Operation.DataManagementOperation]s quickly, since they are usually
     * triggered during an ongoing transaction.
     *
     * @param action The [Operation.DataManagementOperation] that has been reported.
     */
    fun signalEvent(action: Operation.DataManagementOperation)
}