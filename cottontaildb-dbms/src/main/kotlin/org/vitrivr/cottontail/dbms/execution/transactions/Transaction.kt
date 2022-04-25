package org.vitrivr.cottontail.dbms.execution.transactions

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext

/**
 * A [Transaction] that can be used to execute [Operator]s in a given [DefaultQueryContext].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Transaction: TransactionContext {

    /** The [TransactionType] of this [Transaction]. */
    val type: TransactionType

    /** The [TransactionStatus] of this [Transaction]. */
    val state: TransactionStatus

    /**
     * Schedules an [Operator] in the context of this [Transaction] and blocks, until execution has completed.
     *
     * @param operator The [Operator] to execute.
     * @return Resulting [Flow] of [Record]s
     */
    fun execute(operator: Operator): Flow<Record>

    /**
     * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
     */
    fun commit()

    /**
     * Rolls back this [Transaction] thus reverting all operations executed so far.
     */
    fun rollback()

    /**
     * Tries to kill this [Transaction] interrupting all running queries.
     *
     * A call to this method is a best-effort attempt to stop all ongoing queries. After killing a transaction
     * successfully, all changes made through it are rolled back.
     */
    fun kill()
}