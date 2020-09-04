package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.recordset.transform.RecordsetLimitTask
import kotlin.math.min

/**
 * A [NodeExpression] that represents the application of a LIMIT or SKIP clause on the final result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetLimitPhysicalNodeExpression(limit: Long, skip: Long) : AbstractRecordsetPhysicalNodeExpression() {

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

    override fun copy() = RecordsetLimitPhysicalNodeExpression(this.limit, this.skip)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.input.toStage(context))
        stage.addTask(RecordsetLimitTask(this.limit, this.skip))
        return stage
    }
}