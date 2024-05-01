package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.CopyableNode
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An abstract [OperatorNode.Physical] implementation that has no input node, i.e., acts as a source.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class NullaryPhysicalOperatorNode : OperatorNode.Physical(), CopyableNode {
    /** The arity of the [NullaryPhysicalOperatorNode] is always zero. */
    final override val inputArity = 0

    /** A [NullaryPhysicalOperatorNode] is always the base of a tree and thus has the index 0. */
    final override val depth: Int = 0

    /** A [NullaryPhysicalOperatorNode] never has dependencies. */
    final override val dependsOn: Array<GroupId> = emptyArray()

    /** The [base] of a [NullaryPhysicalOperatorNode] is always itself. */
    final override val base: List<Physical> by lazy {
        listOf(this)
    }

    /** The [totalCost] of a [NullaryPhysicalOperatorNode] is always its own [Cost]. */
    context(BindingContext, Tuple)    final override val totalCost: Cost
        get() = this.cost

    /** The [parallelizableCost] of a [NullaryPhysicalOperatorNode] is either [Cost.ZERO] or [cost]. */
    context(BindingContext, Tuple)    final override val parallelizableCost: Cost
        get() = if (this.hasTrait(NotPartitionableTrait)) {
            Cost.ZERO
        } else {
            this.cost
        }

    /** A [NullaryPhysicalOperatorNode] does not have specific [Binding.Column] requirements. */
    final override val requires: List<Binding.Column>
        get() = emptyList()

    /** By default, a [NullaryPhysicalOperatorNode] has an empty set of [Trait]s. */
    override val traits: Map<TraitType<*>,Trait>
        get() = emptyMap()

    /**
     * Creates a copy of this [NullaryPhysicalOperatorNode].
     *
     * @return [NullaryPhysicalOperatorNode].
     */
    abstract override fun copy(): NullaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must be empty!
     * @return Copy of this [NullaryPhysicalOperatorNode].
     */
    final override fun copyWithNewInput(vararg input: Physical): NullaryPhysicalOperatorNode {
        require(input.isEmpty()) { "The input arity for NullaryPhysicalOperatorNode.copyWithNewInput() must be 0 but is ${input.size}. This is a programmer's error!"}
        return this.copy()
    }

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode].
     *
     * @param replacements The list of replacements. Must be empty!
     * @return Copy of this [NullaryPhysicalOperatorNode].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Physical): NullaryPhysicalOperatorNode {
        require(replacements.isEmpty()) { "The input arity for NullaryPhysicalOperatorNode.copyWithExistingGroupInput() must be 0 but is ${replacements.size}. This is a programmer's error!"}
        return this.copy()
    }

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode].
     *
     * @return Copy of this [NullaryPhysicalOperatorNode].
     */
    final override fun copyWithExistingInput(): NullaryPhysicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] and its entire output [OperatorNode.Physical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must be empty!
     * @return Copy of this [NullaryPhysicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Physical): NullaryPhysicalOperatorNode {
        require(input.isEmpty()) { "The input arity for NullaryPhysicalOperatorNode.copyWithOutput() must be 0 but is ${input.size}. This is a programmer's error!"}
        val copy = this.copyWithExistingInput()
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * By default, a [NullaryPhysicalOperatorNode] cannot be partitioned and hence this method returns null.
     *
     * Must be overridden in order to support partitioning.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? = null

    /**
     * By default, [NullaryPhysicalOperatorNode] cannot be partitioned and hence calling this method throws an exception.
     *
     * @param partitions The total number of partitions.
     * @param p The partition index.
     * @return null
     */
    override fun partition(partitions: Int, p: Int): Physical
        = throw UnsupportedOperationException("NullaryPhysicalOperatorNode cannot be partitioned!")

    /**
     * Calculates and returns the total [Digest] for this [NullaryPhysicalOperatorNode].
     *
     * @return Total [Digest] for this [NullaryPhysicalOperatorNode]
     */
    final override fun totalDigest(): Digest = this.digest()
}