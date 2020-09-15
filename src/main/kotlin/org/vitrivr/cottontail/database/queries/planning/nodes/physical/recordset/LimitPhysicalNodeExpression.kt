package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import kotlin.math.min

/**
 * A [NodeExpression] that represents the application of a LIMIT or SKIP clause on the final result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class LimitPhysicalNodeExpression(limit: Long, skip: Long) : AbstractRecordsetPhysicalNodeExpression() {

    val limit = if (limit.coerceAtLeast(0) == 0L) {
        Long.MAX_VALUE
    } else {
        limit
    }

    val skip = if (limit.coerceAtLeast(0) == 0L) {
        0L
    } else {
        skip
    }

    override val outputSize: Long
        get() = min((this.input.outputSize - this.skip), this.limit)

    override val cost: Cost
        get() = Cost(cpu = this.outputSize * 1e-5f, memory = this.input.let { (it.cost.memory / it.outputSize) * this.outputSize })

    override fun copy() = LimitPhysicalNodeExpression(this.limit, this.skip)

    override fun toOperator(context: ExecutionEngine.ExecutionContext): ProducingOperator = LimitOperator(this.input.toOperator(context), context, this.skip, this.limit)
}