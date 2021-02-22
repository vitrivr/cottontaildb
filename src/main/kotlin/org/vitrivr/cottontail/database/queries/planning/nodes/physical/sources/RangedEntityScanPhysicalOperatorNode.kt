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
 * @version 1.2.0
 */
class RangedEntityScanPhysicalOperatorNode(val entity: Entity, override val columns: Array<ColumnDef<*>>, val range: LongRange) : NullaryPhysicalOperatorNode() {
    init {
        require(this.range.first >= 0L) { "Start of a ranged entity scan must be greater than zero." }
    }

    override val outputSize = (this.range.last - this.range.first)
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = Cost(
        this.columns.map { this.outputSize * it.type.physicalSize * Cost.COST_DISK_ACCESS_READ }.sum(),
        this.columns.map { this.outputSize * it.type.physicalSize * Cost.COST_MEMORY_ACCESS }.sum()
    )

    override fun copy() =
        RangedEntityScanPhysicalOperatorNode(this.entity, this.columns, this.range)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        EntityScanOperator(this.entity, this.columns, this.range)

    override fun partition(p: Int): List<OperatorNode.Physical> {
        throw IllegalStateException("RangedEntityScanPhysicalNodeExpression cannot be further partitioned.")
    }
}