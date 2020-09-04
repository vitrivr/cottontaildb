package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.recordset.knn.RecordsetScanKnnTask

/**
 * A [NodeExpression] that represents the application of a [KnnPredicate] on some intermediate result [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetKnnPhysicalNodeExpression(val knn: KnnPredicate<*>) : AbstractRecordsetPhysicalNodeExpression() {
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(
                cpu = this.input.outputSize * this.knn.cost,
                memory = (this.outputSize * this.knn.columns.map { it.physicalSize }.sum()).toFloat()
        )

    override fun copy() = RecordsetKnnPhysicalNodeExpression(this.knn)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE, this.input.toStage(context))
        stage.addTask(RecordsetScanKnnTask(this.knn))
        return stage
    }
}

