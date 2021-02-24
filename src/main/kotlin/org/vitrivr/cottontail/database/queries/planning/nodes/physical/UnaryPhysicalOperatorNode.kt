package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Binding
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.sort.SortOrder

/**
 * An abstract [PhysicalOperatorNode] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class UnaryPhysicalOperatorNode(val input: OperatorNode.Physical) : OperatorNode.Physical() {

    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 1

    /** The [base] of a [UnaryPhysicalOperatorNode] is always its parent's base. */
    final override val base: Collection<OperatorNode.Physical>
        get() = this.input.base

    /** The [totalCost] of a [UnaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() = this.input.totalCost + this.cost

    /** By default, a [UnaryPhysicalOperatorNode]'s requirements are unspecified. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /** [OperatorNode.Physical]s are executable if all their inputs are executable. */
    override val executable: Boolean
        get() = this.input.executable

    /** [UnaryPhysicalOperatorNode] can be partitioned, if its parent can be partitioned. */
    override val canBePartitioned: Boolean
        get() = this.input.canBePartitioned

    /** By default, a [UnaryPhysicalOperatorNode]'s order is retained. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>>
        get() = this.input.order

    init {
        this.input.output = this
    }

    /**
     * Performs late value binding using the given [QueryContext].
     *
     * [OperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]
     * of this [OperatorNode].
     *
     * @param ctx [QueryContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.input.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [UnaryPhysicalOperatorNode].
     *
     * @return Digest for this [UnaryPhysicalOperatorNode]
     */
    final override fun digest(): Long = 27L * this.hashCode() + this.input.digest()
}