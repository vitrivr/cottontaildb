package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An abstract [OperatorNode.Physical] implementation that has no input node, i.e., acts as a source.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class NullaryPhysicalOperatorNode : OperatorNode.Physical() {
    /** The arity of the [NullaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 0

    /** The [base] of a [NullaryPhysicalOperatorNode] is always itself. */
    final override val base: Collection<OperatorNode.Physical>
        get() = listOf(this)

    /** The [totalCost] of a [NullaryPhysicalOperatorNode] is always its own [Cost]. */
    final override val totalCost: Cost
        get() = this.cost

    /** By default, a [NullaryPhysicalOperatorNode]'s order is retained. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [NullaryPhysicalOperatorNode]'s requirements are unspecified. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /**
     * Performs late value binding using the given [QueryContext].
     *
     * By default, this operation has no effect. Override to implement operator specific binding.
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode = this

    /**
     * Calculates and returns the digest for this [NullaryPhysicalOperatorNode].
     *
     * @return Digest for this [NullaryPhysicalOperatorNode]
     */
    final override fun digest(): Long = this.hashCode().toLong()

    /** Generates and returns a [String] representation of this [OperatorNode]. */
    override fun toString() = "${this.javaClass.simpleName}[*${this.groupId}]"
}