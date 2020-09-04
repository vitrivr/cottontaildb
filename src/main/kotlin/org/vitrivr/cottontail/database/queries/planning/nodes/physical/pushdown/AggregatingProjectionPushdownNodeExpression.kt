package org.vitrivr.cottontail.database.queries.planning.nodes.physical.pushdown

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.components.Projection
import org.vitrivr.cottontail.database.queries.planning.QueryPlannerContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.AbstractEntityPhysicalNodeExpression
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionStage
import org.vitrivr.cottontail.execution.tasks.entity.projection.*
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [NodeExpression] that represents a aggregating projection on a physical entity. Combining this
 * with the actual read operation in the [Entity] is usually more efficient than fetching all data into
 * memory and then performing the filtering on that data.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class AggregatingProjectionPushdownNodeExpression(val type: Projection, val entity: Entity, val column: ColumnDef<*>? = null, val alias: Name.ColumnName? = null) : AbstractEntityPhysicalNodeExpression() {

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
            else -> throw IllegalArgumentException("AggregatingProjectionPushdownNodeExpression cannot be used for projection of type $type.")
        }
    }

    /** [Cost] of executing this [KnnPushdownPhysicalNodeExpression]. */
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = when (type) {
            Projection.COUNT -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.COUNT_DISTINCT -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, (this.column!!.physicalSize).toFloat())
            Projection.EXISTS -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.SUM -> Cost(this.entity.statistics.rows * Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.MAX -> Cost(this.entity.statistics.rows * Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.MIN -> Cost(this.entity.statistics.rows * Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            Projection.MEAN -> Cost(this.entity.statistics.rows * Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, 0.0f)
            else -> Cost.INVALID
        }

    override fun copy() = AggregatingProjectionPushdownNodeExpression(this.type, this.entity, this.column, this.alias)

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