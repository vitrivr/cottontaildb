package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class EntityScanPhysicalOperatorNode(override val groupId: Int, val entity: Entity, override val columns: Array<ColumnDef<*>>) : NullaryPhysicalOperatorNode() {
    override val statistics: RecordStatistics = this.entity.statistics
    override val outputSize = this.entity.numberOfRows
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = true
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.map {
        this.statistics[it].avgWidth
    }.sum()

    /**
     * Returns a copy of this [EntityScanPhysicalOperatorNode].
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copyWithInputs() = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns)

    /**
     * Returns a copy of this [EntityScanPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [EntityScanPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val scan = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns)
        return (this.output?.copyWithOutput(scan) ?: scan)
    }

    /**
     * Partitions this [EntityScanPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        val partitionSize = Math.floorDiv(this.outputSize, p.toLong())
        return (0 until p).map {
            val start = (it * partitionSize)
            val end = ((it + 1) * partitionSize) - 1
            RangedEntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns, start..end)
        }
    }

    /**
     * Converts this [EntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntityScanOperator(this.groupId, this.entity, this.columns, 0L..this.entity.maxTupleId)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityScanPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        return result
    }
}