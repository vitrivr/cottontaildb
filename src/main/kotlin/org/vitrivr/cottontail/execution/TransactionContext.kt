package org.vitrivr.cottontail.execution

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.Lock
import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TransactionId

/**
 * A [TransactionContext] used by operators and their subtransactions to execute and obtain necessary locks
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
interface TransactionContext {

    /** The [TransactionId] of this [TransactionContext]. */
    val txId: TransactionId

    /** The [TransactionType] of this [TransactionContext]. */
    val type: TransactionType

    /** The [TransactionContext] of this [TransactionContext]. */
    val state: TransactionStatus

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

    /**
     * Schedules an [Operator] for execution in this [TransactionContext] and blocks, until execution has completed.
     *
     * @param context The [QueryContext] to execute the [Operator] in.
     * @param operator The [Operator.SinkOperator] that should be executed.
     * @return Resulting [Flow] of [Record]s
     */
    fun execute(operator: Operator, context: QueryContext): Flow<Record>
}