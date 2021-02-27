package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An abstract [NaryPhysicalOperatorNode] implementation that has multiple [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class NAryPhysicalOperatorNode(vararg val inputs: OperatorNode.Physical) : OperatorNode.Physical() {

    init {
        require(inputs.size > 1) { "The use of an NAryPhysicalOperatorNode requires at least two inputs." }
    }

    /** Input arity of [BinaryPhysicalOperatorNode] is always two. */
    final override val inputArity: Int
        get() = this.inputs.size

    /**
     * The group ID of a [BinaryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: Int
        get() = this.inputs[0].groupId

    /** The [base] of a [BinaryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<OperatorNode.Physical>
        get() = this.inputs.flatMap { it.base }

    /** The [totalCost] of a [BinaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() {
            var cost = this.cost
            for (i in inputs) {
                cost += i.totalCost
            }
            return cost
        }

    /** By default, a [BinaryPhysicalOperatorNode]'s order is unspecified. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [BinaryPhysicalOperatorNode]'s requirements are empty. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /** [UnaryPhysicalOperatorNode] can be partitioned, if its parent can be partitioned. */
    override val canBePartitioned: Boolean = false

    /** By default, a [UnaryPhysicalOperatorNode]'s [RecordStatistics] is retained. */
    override val statistics: RecordStatistics
        get() = this.inputs[0].statistics

    init {
        this.inputs.forEach { it.output = this }
    }

    /**
     * Performs late value binding using the given [BindingContext].
     *
     * [NAryPhysicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]s.
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.inputs.forEach { it.bindValues(ctx) }
        return this
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return Digest for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Long {
        var result = 27L * hashCode()
        for (i in this.inputs) {
            result += 27L * i.digest()
        }
        return result
    }
}