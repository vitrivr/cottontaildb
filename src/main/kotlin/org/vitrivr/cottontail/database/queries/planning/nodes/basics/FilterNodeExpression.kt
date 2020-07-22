package org.vitrivr.cottontail.database.queries.planning.nodes.basics

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.recordset.filter.RecordsetScanFilterTask

/**
 * A [NodeExpression] that represents application of a [BooleanPredicate] on some intermediate result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FilterNodeExpression(val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractNodeExpression() {
    override val output: Long
        get() = ((this.parents.firstOrNull()?.output ?: 0) * this.selectivity).toLong()

    override val cost: Cost
        get() = Cost(
                0.0f,
                (this.parents.firstOrNull()?.output ?: 0L) * this.predicate.cost,
                (this.output * this.predicate.columns.map { it.physicalSize }.sum()).toFloat()
        )

    override fun copy(): NodeExpression = FilterNodeExpression(this.predicate, this.selectivity)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.parents.first().toStage(context))
        stage.addTask(RecordsetScanFilterTask(this.predicate))
        return stage
    }
}