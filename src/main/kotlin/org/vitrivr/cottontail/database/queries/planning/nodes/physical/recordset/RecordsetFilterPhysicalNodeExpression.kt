package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
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
class RecordsetFilterPhysicalNodeExpression(val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractRecordsetPhysicalNodeExpression() {

    override val outputSize: Long
        get() = (this.input.outputSize * this.selectivity).toLong()

    override val cost: Cost
        get() = Cost(
                cpu = this.input.outputSize * this.predicate.cost,
                memory = (this.outputSize * this.predicate.columns.map { it.physicalSize }.sum()).toFloat()
        )

    override fun copy() = RecordsetFilterPhysicalNodeExpression(this.predicate, this.selectivity)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.input.toStage(context))
        stage.addTask(RecordsetScanFilterTask(this.predicate))
        return stage
    }
}