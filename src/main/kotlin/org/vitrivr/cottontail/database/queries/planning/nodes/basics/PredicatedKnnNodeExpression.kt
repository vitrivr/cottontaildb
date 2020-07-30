package org.vitrivr.cottontail.database.queries.planning.nodes.basics

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.recordset.knn.RecordsetScanKnnTask

/**
 * A [NodeExpression] that represents the sequential application of [BooleanPredicate] and a [KnnPredicate] on some intermediate result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class PredicatedKnnNodeExpression(val knn: KnnPredicate<*>, val predicate: BooleanPredicate) : AbstractNodeExpression() {
    override val output: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(
                0.0f,
                (this.parents.firstOrNull()?.output ?: 0L) * (this.knn.cost + this.predicate.cost),
                (this.output * (this.knn.columns.map { it.physicalSize }.sum() + this.predicate.columns.map { it.physicalSize }.sum())).toFloat()
        )

    override fun copy(): NodeExpression = PredicatedKnnNodeExpression(this.knn, this.predicate)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.parents.first().toStage(context))
        stage.addTask(RecordsetScanKnnTask(this.knn, this.predicate))
        return stage
    }
}
