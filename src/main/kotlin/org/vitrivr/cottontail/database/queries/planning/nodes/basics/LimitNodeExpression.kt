package org.vitrivr.cottontail.database.queries.planning.nodes.basics

import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
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
class LimitNodeExpression(limit: Long, skip: Long) : AbstractNodeExpression() {

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

    override val output: Long
        get() = min((parents.first().output - this.skip), this.limit)

    override val cost: Cost
        get() = Cost(
                0.0f,
                this.output * 1e-5f,
                (parents.first().cost.memory / parents.first().output) * this.output
        )

    override fun copy(): NodeExpression = LimitNodeExpression(this.limit, this.skip)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.parents.first().toStage(context))
        stage.addTask(RecordsetLimitTask(this.limit, this.skip))
        return stage
    }
}