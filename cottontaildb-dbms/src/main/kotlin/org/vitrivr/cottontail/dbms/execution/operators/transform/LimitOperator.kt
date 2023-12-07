package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.basics.take
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Limit the number of outgoing [Tuple]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LimitOperator(parent: Operator, private val limit: Long, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns returned by [LimitOperator] depend on the parent [Operator]. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** [LimitOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [LimitOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [LimitOperator]
     */
    override fun toFlow(): Flow<Tuple>
        = this.parent.toFlow().take(this.limit)
}