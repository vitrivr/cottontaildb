package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An [Operator.PipelineOperator] used during query execution. It counts the number of rows it
 * encounters and returns the value as [Tuple].
 *
 * Only produces a single [Tuple]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CountProjectionOperator(parent: Operator, override val context: QueryContext) : Operator.PipelineOperator(parent) {
    /** Column returned by [CountProjectionOperator]. */
    override val columns: List<ColumnDef<*>>
        = listOf(ColumnDef(Name.ColumnName.create(Projection.COUNT.column()), Types.Long))

    /** [CountProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [CountProjectionOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@CountProjectionOperator.parent.toFlow()
        emit(StandaloneTuple(0L, this@CountProjectionOperator.columns[0], LongValue(incoming.count())))
    }
}