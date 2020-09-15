package org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator

/**
 * A [AbstractEntityPhysicalNodeExpression] that represents a predicated lookup using an [Index].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class IndexScanPhysicalNodeExpression(val entity: Entity, val index: Index, val predicate: BooleanPredicate, val selectivity: Float = Costs.DEFAULT_SELECTIVITY) : AbstractEntityPhysicalNodeExpression() {

    /** [Cost] of executing this [KnnPushdownPhysicalNodeExpression]. */
    override val outputSize: Long
        get() = (this.entity.statistics.rows * this.selectivity).toLong()

    override val cost: Cost
        get() = this.index.cost(this.predicate)

    override fun copy() = IndexScanPhysicalNodeExpression(this.entity, this.index, this.predicate, this.selectivity)

    override fun toOperator(context: ExecutionEngine.ExecutionContext): ProducingOperator {
        TODO("Not yet implemented")
    }
}