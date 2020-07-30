package org.vitrivr.cottontail.database.queries.planning.nodes.pushdown

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.filter.EntityIndexedFilterTask

/**
 * A [NodeExpression] that represents a linear predicated lookup on a physical entity. Combining filtering
 * with the actual read operation in the [Entity] is usually more efficient than fetching all data into memory
 * and then performing the filtering on that data.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class PredicatePushdownWithIndexNodeExpression(val entity: Entity, val index: Index, val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractNodeExpression() {

    /** [Cost] of executing this [KnnPushdownNodeExpression]. */
    override val output: Long
        get() = ((this.parents.firstOrNull()?.output ?: 0) * this.selectivity).toLong()

    override val cost: Cost
        get() = this.index.cost(this.predicate)

    override fun copy(): NodeExpression = PredicatePushdownWithIndexNodeExpression(this.entity, this.index, this.predicate, this.selectivity)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
        stage.addTask(EntityIndexedFilterTask(this.entity, this.predicate, this.index))
        return stage
    }
}