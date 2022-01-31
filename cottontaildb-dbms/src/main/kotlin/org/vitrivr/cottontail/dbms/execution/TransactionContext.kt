package org.vitrivr.cottontail.dbms.execution

import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.locking.Lock
import org.vitrivr.cottontail.dbms.locking.LockManager
import org.vitrivr.cottontail.dbms.locking.LockMode
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.core.database.TransactionId

/**
 * A [TransactionContext] used by operators and their sub transactions to execute and obtain necessary locks on database objects.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
interface TransactionContext {

    /** The [TransactionId] of this [TransactionContext]. */
    val txId: TransactionId

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