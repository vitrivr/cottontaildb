package org.vitrivr.cottontail.database.queries.planning.nodes.pushdown

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.knn.EntityScanKnnTask
import org.vitrivr.cottontail.execution.tasks.recordset.merge.RecordsetMergeKnnTask

/**
 * A [NodeExpression] that represents a linear scan kNN on a physical entity (optionally combined
 * with a [BooleanPredicate]). Combining a kNN lookup with the actual read operation in the [Entity]
 * is usually more efficient than fetching all data into memory and then performing the lookup on that data.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnPushdownNodeExpression(val entity: Entity, val knn: KnnPredicate<*>, val predicate: BooleanPredicate? = null) : AbstractNodeExpression() {

    /** [Cost] of executing this [KnnPushdownNodeExpression]. */
    override val output: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(
                this.entity.statistics.rows * Costs.DISK_ACCESS_READ,
                this.entity.statistics.rows * (this.knn.cost + (this.predicate?.cost ?: 0.0f)),
                (this.output * (this.knn.columns.map { it.physicalSize }.sum() + (this.predicate?.columns?.map { it.physicalSize }?.sum()
                        ?: 0))).toFloat()
        )

    override fun copy(): NodeExpression = KnnPushdownNodeExpression(this.entity, this.knn, this.predicate)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        /* Add default case 1: Full table scan based Knn. */
        val parallelism = kotlin.math.ceil(this.cost.cpu).toInt().coerceAtLeast(1).coerceAtMost(context.parallelism)
        val maxTupleId = this.entity.statistics.maxTupleId
        val blocksize = maxTupleId / parallelism

        /** Prepare kNN stage. */
        val knnStage = ExecutionStage(mergeType = ExecutionStage.MergeType.ONE)
        for (i in 0 until parallelism) {
            knnStage.addTask(EntityScanKnnTask(this.entity, this.knn, this.predicate, blocksize * i + 1L, (i + 1) * blocksize))
        }

        /** Add a merge stage, if parallelism is > 1. */
        return if (parallelism > 1) {
            ExecutionStage(ExecutionStage.MergeType.ALL, knnStage).addTask(RecordsetMergeKnnTask(this.entity, this.knn)) /* Create and return a merge stage. */
        } else {
            knnStage
        }
    }
}