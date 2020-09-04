package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.recordset.knn.RecordsetScanKnnTask

/**
 * A [NodeExpression] that represents the sequential application of [BooleanPredicate] and a [KnnPredicate] on some intermediate result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetPredicatedKnnPhysicalNodeExpression(val knn: KnnPredicate<*>, val predicate: BooleanPredicate) : AbstractRecordsetPhysicalNodeExpression() {
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(
                cpu = this.input.outputSize * (this.knn.cost + this.predicate.cost),
                memory = this.outputSize * (this.knn.columns.map { it.physicalSize }.sum() + this.predicate.columns.map { it.physicalSize }.sum()).toFloat()
        )

    override fun copy() = RecordsetPredicatedKnnPhysicalNodeExpression(this.knn, this.predicate)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.input.toStage(context))
        stage.addTask(RecordsetScanKnnTask(this.knn, this.predicate))
        return stage
    }
}