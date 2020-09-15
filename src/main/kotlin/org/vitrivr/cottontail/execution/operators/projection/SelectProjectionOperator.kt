package org.vitrivr.cottontail.execution.operators.projection

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.PipelineOperator
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineBreaker] used during query execution. It tracks the minimum value it has
 * encountered so far  and returns it as a [Record]. The [MinProjectionOperator] retains the type
 * of the incoming records.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SelectProjectionOperator(parent: ProducingOperator, context: ExecutionEngine.ExecutionContext, val fields: List<Pair<ColumnDef<*>, Name.ColumnName?>>) : PipelineOperator(parent, context) {

    override val columns: Array<ColumnDef<*>> = fields.map { it.first }.toTypedArray()

    override fun getNext(input: Record?): Record? = input
}