package org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.knn.IndexScanKnnTask

/**
 * A [NodeExpression.PhysicalNodeExpression] that represents a kNN query via an [Index] that support kNN lookups
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class IndexedKnnPhysicalNodeExpression(val entity: Entity, val knn: KnnPredicate<*>, val index: Index) : AbstractEntityPhysicalNodeExpression() {
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = this.index.cost(this.knn)

    override fun copy() = IndexedKnnPhysicalNodeExpression(this.entity, this.knn, this.index)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        return ExecutionStage(ExecutionStage.MergeType.ALL).addTask(IndexScanKnnTask(this.entity, this.knn, this.index))
    }
}