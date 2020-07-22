package org.vitrivr.cottontail.database.queries.planning.nodes.pushdown

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.filter.EntityLinearScanFilterTask

/**
 * A [NodeExpression] that represents a linear predicated lookup on a physical entity. Combining filtering
 * with the actual read operation in the [Entity] is usually more efficient than fetching all data into memory
 * and then performing the filtering on that data.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class PredicatePushdownNodeExpression(val entity: Entity, val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractNodeExpression() {

    /** [Cost] of executing this [KnnPushdownNodeExpression]. */
    override val output: Long
        get() = ((this.parents.firstOrNull()?.output ?: 0) * this.selectivity).toLong()

    override val cost: Cost
        get() = Cost(
                this.entity.statistics.rows * this.predicate.columns.size * Costs.DISK_ACCESS_READ,
                this.entity.statistics.rows * this.predicate.cost,
                (this.output * this.predicate.columns.map { it.physicalSize }.sum()).toFloat()
        )


    override fun copy(): NodeExpression = PredicatePushdownNodeExpression(this.entity, this.predicate, this.selectivity)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
        stage.addTask(EntityLinearScanFilterTask(this.entity, this.predicate))
        return stage
    }
}