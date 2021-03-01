package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Logical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class UnaryLogicalOperatorNode(val input: OperatorNode.Logical) : OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 1

    /** The group Id of a [UnaryLogicalOperatorNode] is always the one of its parent.*/
    final override val groupId: Int
        get() = this.input.groupId

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
     * Performs value binding using the given [QueryContext].
     *
     * [UnaryLogicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all the [input] [OperatorNode].
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.input.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [UnaryLogicalOperatorNode].
     *
     * @return Digest for this [UnaryLogicalOperatorNode]
     */
    final override fun digest(): Long = 33L * this.hashCode() + this.input.digest()

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.input.printTo(p)
        super.printTo(p)
    }
}