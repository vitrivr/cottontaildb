package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Index] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class RangedIndexScanPhysicalOperatorNode(val index: Index, val predicate: Predicate, val range: LongRange) : NullaryPhysicalOperatorNode() {

    init {
        require(this.range.first >= 0L) { "Start of a ranged index scan must be greater than zero." }
    }

    override val columns: Array<ColumnDef<*>> = this.index.produces
    override val outputSize = (this.range.last - this.range.first)
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = this.index.cost(this.predicate)
    override fun copy() = RangedIndexScanPhysicalOperatorNode(this.index, this.predicate, this.range)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = EntityIndexScanOperator(this.index, this.predicate.bindValues(ctx), this.range)
    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    override fun partition(p: Int): List<OperatorNode.Physical> {
        throw IllegalStateException("RangedIndexScanPhysicalOperatorNode cannot be further partitioned.")
    }
}