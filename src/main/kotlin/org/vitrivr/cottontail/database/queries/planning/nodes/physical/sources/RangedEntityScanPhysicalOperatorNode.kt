package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class RangedEntityScanPhysicalOperatorNode(val entity: Entity, override val columns: Array<ColumnDef<*>>, val range: LongRange) : NullaryPhysicalOperatorNode() {
    init {
        require(this.range.first >= 0L) { "Start of a ranged entity scan must be greater than zero." }
    }

    override val outputSize = (this.range.last - this.range.first)
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.map { it.type.physicalSize }.sum()

    /**
     * Returns a copy of this [RangedEntityScanPhysicalOperatorNode].
     *
     * @return Copy of this [RangedEntityScanPhysicalOperatorNode].
     */
    override fun copyWithInputs() = RangedEntityScanPhysicalOperatorNode(this.entity, this.columns, this.range)

    /**
     * Returns a copy of this [RangedEntityScanPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [RangedEntityScanPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val scan = RangedEntityScanPhysicalOperatorNode(this.entity, this.columns, this.range)
        return (this.output?.copyWithOutput(scan) ?: scan)
    }

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<OperatorNode.Physical> {
        throw UnsupportedOperationException("RangedEntityScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /**
     * Converts this [RangedEntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntityScanOperator(this.entity, this.columns, this.range)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedEntityScanPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false
        if (range != other.range) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + range.hashCode()
        return result
    }
}