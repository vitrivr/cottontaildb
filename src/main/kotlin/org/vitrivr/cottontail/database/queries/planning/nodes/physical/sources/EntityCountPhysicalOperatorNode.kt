package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityCountOperator
import org.vitrivr.cottontail.model.basics.Type

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the counting entries in a physical [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class EntityCountPhysicalOperatorNode(val entity: Entity) : NullaryPhysicalOperatorNode() {
    override val outputSize = 1L
    override val statistics: RecordStatistics = this.entity.statistics
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.name.column(Projection.COUNT.label()), Type.Long, false))
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS)

    /**
     * Returns a copy of this [EntityCountPhysicalOperatorNode].
     *
     * @return Copy of this [EntityCountPhysicalOperatorNode].
     */
    override fun copyWithInputs() = EntityCountPhysicalOperatorNode(this.entity)

    /**
     * Returns a copy of this [EntityCountPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [EntityCountPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val count = EntityCountPhysicalOperatorNode(this.entity)
        return (this.output?.copyWithOutput(count) ?: count)
    }

    /**
     * [EntityCountPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        throw UnsupportedOperationException("EntityCountPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Converts this [EntityCountPhysicalOperatorNode] to a [EntityCountOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntityCountOperator(this.entity)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityCountPhysicalOperatorNode) return false
        if (entity != other.entity) return false
        return true
    }

    override fun hashCode(): Int = this.entity.hashCode()
}