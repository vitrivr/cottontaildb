package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [PhysicalOperatorNode] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class UnaryPhysicalOperatorNode(val input: OperatorNode.Physical) : OperatorNode.Physical() {

    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 1

    /** The group Id of a [UnaryPhysicalOperatorNode] is always the one of its parent.*/
    final override val groupId: Int
        get() = this.input.groupId

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

    /** By default, a [UnaryPhysicalOperatorNode]'s [RecordStatistics] is retained. */
    override val statistics: RecordStatistics
        get() = this.input.statistics

    init {
        this.input.output = this
    }

    /**
     * Performs value binding using the given [BindingContext].
     *
     * [UnaryPhysicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all the [input] [OperatorNode].
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.input.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [UnaryPhysicalOperatorNode].
     *
     * @return Digest for this [UnaryPhysicalOperatorNode]
     */
    final override fun digest(): Long = 27L * this.hashCode() + this.input.digest()

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

