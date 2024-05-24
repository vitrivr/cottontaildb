package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Tuple]s for
 * each incoming [Tuple] and removes field not required by the query.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SelectProjectionOperator(parent: Operator, fields: List<Binding.Column>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = fields.map { it.column }

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(): Flow<Tuple> {
        val columns = this@SelectProjectionOperator.columns.toTypedArray()
        return this@SelectProjectionOperator.parent.toFlow().map { r ->
            StandaloneTuple(r.tupleId, columns, Array(columns.size) { r[columns[it]] })
        }
    }
}