package org.vitrivr.cottontail.dbms.queries.operators.physical.transform

import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.transform.SkipOperator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class SkipPhysicalOperatorNode(input: Physical, val skip: Long) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Skip"
    }

    /** The name of this [SkipPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of this [SkipPhysicalOperatorNode], which depends on skip and limit. */
    override val outputSize: Long
        get() = super.outputSize - this.skip

    /** The [Cost] of a [SkipPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * this.outputSize

    /** No partitioning can take place after a [SkipPhysicalOperatorNode] has been introduced. */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + listOf(NotPartitionableTrait to NotPartitionableTrait)


    /**
     * Creates and returns a copy of this [SkipPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [SkipPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): SkipPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SkipPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SkipPhysicalOperatorNode(input = input[0], skip = this.skip)
    }

    /**
     * Converts this [SkipPhysicalOperatorNode] to a [SkipOperator].
     *
     * @param ctx The [TransactionContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = SkipOperator(this.input.toOperator(ctx), this.skip)

    /** Generates and returns a [String] representation of this [SkipPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.skip}]"

    /** Generates and returns a hash code for this [SkipPhysicalOperatorNode]. */
    override fun hashCode(): Int = this.skip.hashCode() + 2

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitPhysicalOperatorNode) return false
        if (this.skip != other.limit) return false
        return true
    }
}