package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An [Operator.PipelineOperator] used during query execution. It returns a single [Tuple] containing
 * either true or false depending on whether there are incoming [Tuple]s.
 *
 * Only produces a single [Tuple]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ExistsProjectionOperator(parent: Operator, val out: Binding.Column, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Column returned by [ExistsProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = listOf(ColumnDef(Name.ColumnName(Projection.EXISTS.column()), Types.Boolean))

    /** [ExistsProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [ExistsProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [ExistsProjectionOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@ExistsProjectionOperator.parent.toFlow()
        val exists = incoming.firstOrNull() != null
        emit(StandaloneTuple(0L, this@ExistsProjectionOperator.columns[0], BooleanValue(exists)))
    }
}