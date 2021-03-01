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
 * An abstract [BinaryPhysicalOperatorNode] implementation that has exactly two [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class BinaryPhysicalOperatorNode(val left: OperatorNode.Physical, val right: OperatorNode.Physical) : OperatorNode.Physical() {

    /** Input arity of [BinaryPhysicalOperatorNode] is always two. */
    final override val inputArity: Int = 2

    /**
     * The group ID of a [BinaryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: Int
        get() = this.left.groupId

    /** The [base] of a [BinaryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<OperatorNode.Physical>
        get() = this.left.base + this.right.base

    /** The [totalCost] of a [BinaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() = this.left.totalCost + this.right.totalCost + this.cost

    /** By default, a [BinaryPhysicalOperatorNode]'s order is unspecified. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [BinaryPhysicalOperatorNode]'s requirements are empty. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /** [UnaryPhysicalOperatorNode] can be partitioned, if its parent can be partitioned. */
    override val canBePartitioned: Boolean = false

    /** By default, a [UnaryPhysicalOperatorNode]'s [RecordStatistics] is retained. */
    override val statistics: RecordStatistics
        get() = this.left.statistics

    init {
        this.left.output = this
        this.right.output = this
    }

    /**
     * Performs value binding using the given [BindingContext].
     *
     * [OperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to [left] and [right] input [OperatorNode].
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.left.bindValues(ctx)
        this.right.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return Digest for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Long {
        val result = 27L * hashCode() + this.left.digest()
        return 27L * result + this.right.digest()
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