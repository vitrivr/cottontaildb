package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Binding
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder

/**
 * An abstract [BinaryLogicalOperatorNode] implementation that has exactly two [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class BinaryLogicalOperatorNode(val input1: OperatorNode.Logical, val input2: OperatorNode.Logical) : OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 2

    /** The [base] of a [BinaryLogicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<OperatorNode.Logical>
        get() = this.input1.base + this.input2.base

    /** By default, a [BinaryLogicalOperatorNode]'s order is unspecified. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [BinaryLogicalOperatorNode]'s requirements are empty. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    init {
        this.input1.output = this
        this.input2.output = this
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
        this.input1.bindValues(ctx)
        this.input2.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [BinaryLogicalOperatorNode].
     *
     * @return Digest for this [BinaryLogicalOperatorNode]
     */
    override fun digest(): Long {
        val result = 33L * hashCode() + this.input1.digest()
        return 33L * result + this.input2.digest()
    }
}