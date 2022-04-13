package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.basics.drop
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.PipelineOperator] used during query execution. Skips incoming [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SkipOperator(parent: Operator, val skip: Long) : Operator.PipelineOperator(parent) {

    /** Columns returned by [SkipOperator] depend on the parent [Operator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** [SkipOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SkipOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SkipOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record>
        = this.parent.toFlow(context).drop(this.skip)
}