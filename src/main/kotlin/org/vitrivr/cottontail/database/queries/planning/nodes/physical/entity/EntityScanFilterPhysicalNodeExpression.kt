package org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.filter.EntityLinearScanFilterTask

/**
 * An [AbstractEntityPhysicalNodeExpression] that represents a linear, predicated lookup on a physical entity.
 * Combining filtering  with the actual read operation in the [Entity] is usually more efficient than fetching
 * all data into memory and then performing the filtering on that data.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class EntityScanFilterPhysicalNodeExpression(val entity: Entity, val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractEntityPhysicalNodeExpression() {

    /** Expected output size for this [EntityScanFilterPhysicalNodeExpression]. */
    override val outputSize: Long
        get() = (this.entity.statistics.rows * this.selectivity).toLong()

    /** [Cost] of executing this [EntityScanFilterPhysicalNodeExpression]. */
    override val cost: Cost
        get() = Cost(
                this.entity.statistics.rows * this.predicate.columns.size * Costs.DISK_ACCESS_READ,
                this.entity.statistics.rows * this.predicate.cost,
                (this.outputSize * this.predicate.columns.map { it.physicalSize }.sum()).toFloat()
        )


    override fun copy() = EntityScanFilterPhysicalNodeExpression(this.entity, this.predicate, this.selectivity)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
        stage.addTask(EntityLinearScanFilterTask(this.entity, this.predicate))
        return stage
    }
}