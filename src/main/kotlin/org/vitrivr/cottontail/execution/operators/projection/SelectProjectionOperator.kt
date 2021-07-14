package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes field not required by the query.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class SelectProjectionOperator(parent: Operator, fields: List<Name.ColumnName>) : Operator.PipelineOperator(parent) {

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parent.columns.filter { c -> fields.any { f -> f == c.name }}

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: QueryContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(columns.size)
        return this.parent.toFlow(context).map { r ->
            columns.forEachIndexed { i, c -> values[i] = r[c]  }
            StandaloneRecord(r.tupleId, columns, values)
        }
    }
}