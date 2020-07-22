package org.vitrivr.cottontail.database.queries.planning.nodes.pushdown

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.components.Projection
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.basics.AbstractNodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.projection.*
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [NodeExpression] that represents a aggregating projection on a physical entity. Combining this
 * with the actual read operation in the [Entity] is usually more efficient than fetching all data into
 * memory and then performing the filtering on that data.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class AggregatingProjectionPushdownNodeExpression(val type: Projection, val entity: Entity, val column: ColumnDef<*>? = null, val alias: String? = null) : AbstractNodeExpression() {

    init {
        /* Sanity check. */
        when (this.type) {
            Projection.COUNT -> {
            }
            Projection.COUNT_DISTINCT -> if (this.column == null) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            }
            Projection.MEAN,
            Projection.SUM,
            Projection.MIN,
            Projection.MAX -> if (this.column == null) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (this.column.type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied on a numeric column, which ${this.column.name} is not.")
            }
            else -> throw IllegalArgumentException("ProjectionPushdownNodeExpression cannot be used for projection of type $type.")
        }
    }

    /** [Cost] of executing this [KnnPushdownNodeExpression]. */
    override val output: Long
        get() = ((this.parents.firstOrNull()?.output ?: 0)).toLong()

    override val cost: Cost
        get() = when (type) {
            Projection.COUNT -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.COUNT_DISTINCT -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, (this.output * this.column!!.physicalSize).toFloat())
            Projection.EXISTS -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.SUM -> Cost(this.output * Costs.DISK_ACCESS_READ, this.output * Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.MAX -> Cost(this.output * Costs.DISK_ACCESS_READ, this.output * Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.MIN -> Cost(this.output * Costs.DISK_ACCESS_READ, this.output * Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.MEAN -> Cost(this.output * Costs.DISK_ACCESS_READ, this.output * Costs.MEMORY_ACCESS_READ, 0.0f)
            else -> Cost.INVALID
        }

    override fun copy(): NodeExpression = AggregatingProjectionPushdownNodeExpression(this.type, this.entity, this.column, this.alias)

    override fun toStage(context: QueryPlannerContext): ExecutionStage {
        val stage = ExecutionStage(ExecutionStage.MergeType.ONE)
        when (this.type) {
            Projection.COUNT -> stage.addTask(EntityCountProjectionTask(this.entity))
            Projection.EXISTS -> stage.addTask(EntityExistsProjectionTask(this.entity))
            Projection.SUM -> stage.addTask(EntitySumProjectionTask(this.entity, this.column!!, this.alias))
            Projection.MAX -> stage.addTask(EntityMaxProjectionTask(this.entity, this.column!!, this.alias))
            Projection.MIN -> stage.addTask(EntityMinProjectionTask(this.entity, this.column!!, this.alias))
            Projection.MEAN -> stage.addTask(EntityMeanProjectionTask(this.entity, this.column!!, this.alias))
            else -> {
            }
        }
        return stage
    }
}