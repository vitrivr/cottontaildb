package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.basics.drop
import org.vitrivr.cottontail.dbms.execution.operators.basics.take
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.PipelineOperator] used during query execution. Limit the number of outgoing [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class LimitOperator(parent: Operator, val skip: Long, val limit: Long) : Operator.PipelineOperator(parent) {

    /** Columns returned by [LimitOperator] depend on the parent [Operator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** [LimitOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [LimitOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [LimitOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        var ret = this.parent.toFlow(context)
        if (this.skip > 0)
            ret = ret.drop(this.skip)
        if (this.limit > 0)
            ret = ret.take(this.limit)
        return ret
    }
}