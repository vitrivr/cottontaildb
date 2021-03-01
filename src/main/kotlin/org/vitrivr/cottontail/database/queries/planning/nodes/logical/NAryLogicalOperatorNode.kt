package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [NAryLogicalOperatorNode] implementation that has multiple [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class NAryLogicalOperatorNode(vararg val inputs: OperatorNode.Logical) : OperatorNode.Logical() {

    init {
        require(inputs.size > 1) { "The use of an NAryLogicalOperatorNode requires at least two inputs." }
    }

    /** Input arity of [NAryLogicalOperatorNode] depends on the number of inputs. */
    final override val inputArity: Int = inputs.size

    /**
     * The group ID of a [NAryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: Int
        get() = this.inputs[0].groupId

    /** The [base] of a [NAryLogicalOperatorNode] is always itself. */
    final override val base: Collection<OperatorNode.Logical>
        get() = this.inputs.flatMap { it.base }

    /** By default, a [NAryLogicalOperatorNode]'s output is unordered. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [NAryLogicalOperatorNode] doesn't have any requirement. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    init {
        this.inputs.forEach { it.output = this }
    }

    /**
     * Performs value binding using the given [BindingContext].
     *
     * [NAryLogicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]s.
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [NAryLogicalOperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): NAryLogicalOperatorNode {
        this.inputs.forEach { it.bindValues(ctx) }
        return this
    }

    /**
     * Calculates and returns the digest for this [NAryLogicalOperatorNode].
     *
     * @return Digest for this [NAryLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = this.hashCode().toLong()
        for (i in this.inputs) {
            result = 33L * result + i.digest()
        }
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}