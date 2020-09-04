package org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.filter.EntityIndexedFilterTask

/**
 * A [AbstractEntityPhysicalNodeExpression] that represents a predicated lookup using an [Index].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class IndexedFilterPhysicalNodeExpression(val entity: Entity, val index: Index, val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractEntityPhysicalNodeExpression() {

    /** [Cost] of executing this [KnnPushdownPhysicalNodeExpression]. */
    override val outputSize: Long
        get() = (this.entity.statistics.rows * this.selectivity).toLong()

    override val cost: Cost
        get() = this.index.cost(this.predicate)

    override fun copy() = IndexedFilterPhysicalNodeExpression(this.entity, this.index, this.predicate, this.selectivity)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
        stage.addTask(EntityIndexedFilterTask(this.entity, this.predicate, this.index))
        return stage
    }
}