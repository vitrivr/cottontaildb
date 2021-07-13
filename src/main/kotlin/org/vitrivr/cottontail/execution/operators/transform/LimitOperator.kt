package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.operators.basics.*
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineOperator] used during query execution. Limit the number of outgoing [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
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
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [LimitOperator]
     */
    override fun toFlow(context: QueryContext): Flow<Record> = this.parent.toFlow(context).drop(this.skip).take(this.limit)
}