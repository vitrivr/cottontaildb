package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.predicates.KnnOperator

/**
 * A [NodeExpression] that represents the application of a [KnnPredicate] on some intermediate result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnPhysicalNodeExpression(val knn: KnnPredicate<*>) : AbstractRecordsetPhysicalNodeExpression() {
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(
                cpu = this.input.outputSize * this.knn.cost,
                memory = (this.outputSize * this.knn.columns.map { it.physicalSize }.sum()).toFloat()
        )

    override fun copy() = KnnPhysicalNodeExpression(this.knn)
    override fun toOperator(context: ExecutionEngine.ExecutionContext) = KnnOperator(this.input.toOperator(context), context, this.knn)
}

