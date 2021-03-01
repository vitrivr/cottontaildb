package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [BinaryLogicalOperatorNode] implementation that has exactly two [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class BinaryLogicalOperatorNode(val left: OperatorNode.Logical, val right: OperatorNode.Logical) : OperatorNode.Logical() {

    /** Input arity of [UnaryLogicalOperatorNode] is always two. */
    final override val inputArity: Int = 2

    /**
     * The group ID of a [BinaryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryLogicalOperatorNode]s.
     */
    final override val groupId: Int
        get() = this.left.groupId

    /** The [base] of a [BinaryLogicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<OperatorNode.Logical>
        get() = this.left.base + this.right.base

    /** By default, a [BinaryLogicalOperatorNode]'s order is unspecified. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [BinaryLogicalOperatorNode]'s requirements are empty. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    init {
        this.left.output = this
        this.right.output = this
    }

    /**
     * Performs late value binding using the given [BindingContext].
     *
     * [OperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]
     * of this [OperatorNode].
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.left.bindValues(ctx)
        this.right.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [BinaryLogicalOperatorNode].
     *
     * @return Digest for this [BinaryLogicalOperatorNode]
     */
    override fun digest(): Long {
        val result = 33L * hashCode() + this.left.digest()
        return 33L * result + this.right.digest()
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.left.printTo(p)
        this.right.printTo(p)
        super.printTo(p)
    }
}