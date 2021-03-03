package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An abstract [OperatorNode.Logical] implementation that has no input.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class NullaryLogicalOperatorNode : OperatorNode.Logical() {
    /** Input arity of [NullaryLogicalOperatorNode] is always zero. */
    final override val inputArity: Int = 0

    /** The [base] of a [NullaryLogicalOperatorNode] is always itself. */
    final override val base: Collection<OperatorNode.Logical>
        get() = listOf(this)

    /** By default, a [NullaryLogicalOperatorNode]'s output is unordered. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [NullaryLogicalOperatorNode] doesn't have any requirement. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /**
     * Performs value binding using the given [BindingContext].
     *
     * By default, this operation has no effect. Override to implement operator specific binding.
     *
     * @param ctx [QueryContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode = this

    /**
     * Calculates and returns the digest for this [NullaryLogicalOperatorNode].
     *
     * @return Digest for this [NullaryLogicalOperatorNode]
     */
    final override fun digest(): Long = this.hashCode().toLong()

    /** Generates and returns a [String] representation of this [OperatorNode]. */
    override fun toString() = "${this.javaClass.simpleName}[*${this.groupId}]"
}