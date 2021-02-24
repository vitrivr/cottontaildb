package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.IndexScanOperator

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Index] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.0.0
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

    /**
     * Returns a copy of this [RangedIndexScanPhysicalOperatorNode].
     *
     * @return Copy of this [RangedIndexScanPhysicalOperatorNode].
     */
    override fun copyWithInputs() = RangedIndexScanPhysicalOperatorNode(this.index, this.predicate, this.range)

    /**
     * Returns a copy of this [RangedIndexScanPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [RangedIndexScanPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val scan = RangedIndexScanPhysicalOperatorNode(this.index, this.predicate, this.range)
        return (this.output?.copyWithOutput(scan) ?: scan)
    }

    /**
     * Converts this [RangedIndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = IndexScanOperator(this.index, this.predicate.bindValues(ctx), this.range)

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<OperatorNode.Physical> {
        throw UnsupportedOperationException("RangedIndexScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /**
     * Binds values from the provided [QueryContext] to this [RangedIndexScanPhysicalOperatorNode]'s [Predicate].
     *
     * @param ctx The [QueryContext] used for value binding.
     */
    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedIndexScanPhysicalOperatorNode) return false

        if (index != other.index) return false
        if (predicate != other.predicate) return false
        if (range != other.range) return false

        return true
    }

    override fun hashCode(): Int {
        var result = index.hashCode()
        result = 31 * result + predicate.hashCode()
        result = 31 * result + range.hashCode()
        return result
    }
}