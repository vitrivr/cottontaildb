package org.vitrivr.cottontail.dbms.execution.transactions

import jetbrains.exodus.env.Transaction
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.ExecutionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator

/**
 * A [Transaction] can be used to query and interact with a [Transaction].
 *
 * This is the view of a [Transaction] that is available to the operators that execute a query.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Transaction: ExecutionContext, TransactionMetadata {

    /** The [TransactionManager] this [Transaction] belongs to. */
    val manager: TransactionManager

    /**
     * Schedules an [Operator] in the context of this [Transaction] and blocks, until execution has completed.
     *
     * @param operator The [Operator] to execute.
     * @return Resulting [Flow] of [Tuple]s
     */
    fun execute(operator: Operator): Flow<Tuple>

    /**
     * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
     */
    fun commit()

    /**
     * Rolls back this [Transaction] thus reverting all operations executed so far.
     */
    fun abort()

    /**
     * Tries to kill this [Transaction] interrupting all running queries.
     *
     * A call to this method is a best-effort attempt to stop all ongoing queries. After killing a transaction
     * successfully, all changes made through it are rolled back.
     */
    fun kill()

    /**
     * Registers a [Substransaction] with this [Transaction].
     *
     * @param subTransaction The [SubTransaction] to register.
     */
    fun registerSubtransaction(subTransaction: SubTransaction)

    /**
     * Signals an [Event] to this [Transaction].
     *
     * This method is a facility to communicate actions that take place within a
     * [Transaction] to the 'outside' world. Usually, that communication
     * must be withheld until the [Transaction] commits.
     *
     * @param event The [Event] that has been reported.
     */
    fun signalEvent(event: Event)
}