package org.vitrivr.cottontail.database.queries.planning.nodes.index

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.knn.IndexScanKnnTask

/**
 * A [NodeExpression] that represents a kNN query via an [Index] that support kNN lookups
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnIndexNodeExpression(val entity: Entity, val knn: KnnPredicate<*>, val index: Index) : AbstractNodeExpression() {
    override val output: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = this.index.cost(this.knn)

    override fun copy(): NodeExpression = KnnIndexNodeExpression(this.entity, this.knn, this.index)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        return ExecutionStage(ExecutionStage.MergeType.ALL).addTask(IndexScanKnnTask(this.entity, this.knn, this.index))
    }
}