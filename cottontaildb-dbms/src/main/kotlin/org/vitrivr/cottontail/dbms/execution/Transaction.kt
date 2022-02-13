package org.vitrivr.cottontail.dbms.execution

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.QueryContext

/**
 * A [Transaction] that can be used to execute [Operator]s in a given [QueryContext].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Transaction {
    /** The [TransactionId] of this [Transaction]. */
    val txId: TransactionId

    /** The [TransactionType] of this [Transaction]. */
    val type: TransactionType

    /** The [QueryContext] of this [Transaction]. */
    val state: TransactionStatus

    /** Flag indicating, whether this [Transaction] was used to write any data. */
    val readonly: Boolean

    /**
     * Schedules an [Operator] for execution in this [Transaction] and blocks, until execution has completed.
     *
     * @param context The [QueryContext] to execute the [Operator] in.
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