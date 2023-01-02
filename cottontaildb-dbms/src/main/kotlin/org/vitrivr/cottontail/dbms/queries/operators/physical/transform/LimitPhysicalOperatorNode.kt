package org.vitrivr.cottontail.dbms.queries.operators.physical.transform

import org.vitrivr.cottontail.core.queries.nodes.traits.MaterializedTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.dbms.execution.operators.transform.LimitOperator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT clause on the result.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class LimitPhysicalOperatorNode(input: Physical? = null, val limit: Long) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Limit"
    }

    /** The name of this [LimitPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of this [LimitPhysicalOperatorNode], which depends on skip and limit. */
    override val outputSize: Long
        get() = min((super.outputSize), this.limit)

    /** The [Cost] of a [LimitPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * this.outputSize

    /** No partitioning can take place after a [LimitPhysicalOperatorNode] has been introduced. */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + listOf(NotPartitionableTrait to NotPartitionableTrait)
    /**
     * Creates and returns a copy of this [LimitPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitPhysicalOperatorNode].
     */
    override fun copy() = LimitPhysicalOperatorNode(limit = this.limit)

    /**
     * Tries to create a partitioned version of this [LimitOperator] and its parents.
     *
     * In contrast to the default implementation, this method adjusts the cost of the incoming tree if that tree
     * does not have the [MaterializedTrait]
     *
     * @return Array of [OperatorNode.Physical]s.
     */
    override fun tryPartition(policy: CostPolicy, max: Int): Physical? {
        require(max > 1) { "Expected number of partitions to be greater than one but encountered $max." }
        val input = this.input ?: throw IllegalStateException("Tried to propagate call to tryPartition to an absent input. This is a programmer's error!")

        /** Check: If no materialization takes places upstream, cost must be adjusted by LIMIT. */
        if (!input.hasTrait(MaterializedTrait)) {
            val partitions = policy.parallelisation((this.parallelizableCost / input.outputSize) * this.limit, (this.parallelizableCost / input.outputSize) * this.limit, max)
            if (partitions <= 1) return null
        }

        return super.tryPartition(policy, max)
    }

    /**
     * Converts this [LimitPhysicalOperatorNode] to a [LimitOperator].
     *
     * @param ctx The [TransactionContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = LimitOperator(this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"), this.limit)

    /** Generates and returns a [String] representation of this [LimitPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.limit}]"

    /** Generates and returns a hash code for this [LimitPhysicalOperatorNode]. */
    override fun hashCode(): Int = this.limit.hashCode() + 1

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitPhysicalOperatorNode) return false
        if (this.limit != other.limit) return false
        return true
    }
}