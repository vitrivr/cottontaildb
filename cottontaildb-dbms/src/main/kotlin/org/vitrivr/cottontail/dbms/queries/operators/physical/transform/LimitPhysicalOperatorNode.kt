package org.vitrivr.cottontail.dbms.queries.operators.physical.transform

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.MaterializedTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.recordset.PlaceholderRecord
import org.vitrivr.cottontail.dbms.execution.operators.transform.LimitOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT clause on the result.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class LimitPhysicalOperatorNode(input: Physical, val limit: Long) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Limit"
    }

    /** The name of this [LimitPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of this [LimitPhysicalOperatorNode], which depends on skip and limit. */
    context(BindingContext,Record)    override val outputSize: Long
        get() = min((super.outputSize), this.limit)

    /** The [Cost] of a [LimitPhysicalOperatorNode]. */
    context(BindingContext,Record)
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * this.outputSize

    /** No partitioning can take place after a [LimitPhysicalOperatorNode] has been introduced. */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + listOf(NotPartitionableTrait to NotPartitionableTrait)

    /**
     * Creates and returns a copy of this [LimitPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [LimitPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): LimitPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for LimitPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return LimitPhysicalOperatorNode(input = input[0], limit = this.limit)
    }

    /**
     * Tries to create a partitioned version of this [LimitOperator] and its parents.
     *
     * In contrast to the default implementation, this method adjusts the cost of the incoming tree if that tree
     * does not have the [MaterializedTrait]
     *
     * @param ctx: QueryContext
     * @param max: Int
     * @return Array of [OperatorNode.Physical]s.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        require(max > 1) { "Expected number of partitions to be greater than one but encountered $max." }
        /** Check: If no materialization takes places upstream, cost must be adjusted by LIMIT. */
        with(ctx.bindings) {
            with(PlaceholderRecord) {
                if (!this@LimitPhysicalOperatorNode.input.hasTrait(MaterializedTrait)) {
                    val parallelisableCost = (this@LimitPhysicalOperatorNode.parallelizableCost / this@LimitPhysicalOperatorNode.input.outputSize) * this@LimitPhysicalOperatorNode.limit
                    val totalCost = (this@LimitPhysicalOperatorNode.parallelizableCost / this@LimitPhysicalOperatorNode.input.outputSize) * this@LimitPhysicalOperatorNode.limit
                    val partitions = ctx.costPolicy.parallelisation(parallelisableCost, totalCost, max)
                    if (partitions <= 1) return null
                }
            }
        }
        return super.tryPartition(ctx, max)
    }

    /**
     * Converts this [LimitPhysicalOperatorNode] to a [LimitOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = LimitOperator(this.input.toOperator(ctx), this.limit, ctx)

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