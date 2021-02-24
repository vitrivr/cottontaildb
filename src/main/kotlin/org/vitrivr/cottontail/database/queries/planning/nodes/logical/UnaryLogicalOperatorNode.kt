package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Binding
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder

/**
 * An abstract [OperatorNode.Logical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class UnaryLogicalOperatorNode(val input: OperatorNode.Logical) : OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 1

    /** The [base] of a [UnaryLogicalOperatorNode] is always its [input]'s base. */
    final override val base: Collection<OperatorNode.Logical>
        get() = this.input.base

    /** By default, a [UnaryLogicalOperatorNode]'s order is retained. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = this.input.order

    /** By default, a [UnaryLogicalOperatorNode]'s requirements are unspecified. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

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
     * Calculates and returns the digest for this [UnaryLogicalOperatorNode].
     *
     * @return Digest for this [UnaryLogicalOperatorNode]
     */
    final override fun digest(): Long = 33L * this.hashCode() + this.input.digest()
}