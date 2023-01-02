package org.vitrivr.cottontail.dbms.queries.operators.physical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.NullaryLogicalOperatorNode

/**
 * An abstract [OperatorNode.Physical] implementation that has no input node, i.e., acts as a source.
 *
 * @author Ralph Gasser
 * @version 2.7.0
 */
abstract class NullaryPhysicalOperatorNode : OperatorNode.Physical() {
    /** The arity of the [NullaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 0

    /** A [NullaryPhysicalOperatorNode] is always the root of a tree and thus has the index 0. */
    final override val depth: Int = 0

    /** The [base] of a [NullaryPhysicalOperatorNode] is always itself. */
    final override val base: Collection<Physical>
        get() = listOf(this)

    /** The [totalCost] of a [NullaryPhysicalOperatorNode] is always its own [Cost]. */
    final override val totalCost: Cost
        get() = this.cost

    /** The [parallelizableCost] of a [NullaryPhysicalOperatorNode] is either [Cost.ZERO] or [cost]. */
    final override val parallelizableCost: Cost
        get() {
            return if (this.hasTrait(NotPartitionableTrait)) {
                Cost.ZERO
            } else {
                this.cost
            }
        }

    /** By default, a [NullaryPhysicalOperatorNode] does not have specific requirements. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /** By default, a [NullaryPhysicalOperatorNode] has an empty set of [Trait]s. */
    override val traits: Map<TraitType<*>,Trait>
        get() = emptyMap()

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NullaryPhysicalOperatorNode].
     */
    abstract override fun copy(): NullaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input to this [NullaryPhysicalOperatorNode].
     * @return Copy of this [NullaryPhysicalOperatorNode] with new input.
     */
    final override fun copy(vararg input: Physical): NullaryPhysicalOperatorNode {
        require(input.isEmpty()) { "Cannot provide input for NullaryPhysicalOperatorNode." }
        return this.copy()
    }

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] and its entire output [OperatorNode.Physical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must be empty!
     * @return Copy of this [NullaryPhysicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Physical): Physical {
        val copy = this.copy(*input)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): NullaryPhysicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithInputs(): NullaryPhysicalOperatorNode = this.copy()

    /**
     * By default, a [NullaryPhysicalOperatorNode] cannot be partitioned and hence this method returns null.
     *
     * Must be overridden in order to support partitioning.
     */
    override fun tryPartition(policy: CostPolicy, max: Int): Physical? = null

    /**
     * By default, [NullaryPhysicalOperatorNode] cannot be partitioned and hence calling this method throws an exception.
     *
     * @param partitions The total number of partitions.
     * @param p The partition index.
     * @return null
     */
    override fun partition(partitions: Int, p: Int): Physical {
        throw UnsupportedOperationException("NullaryPhysicalOperatorNode cannot be partitioned!")
    }

    /**
     * Calculates and returns the digest for this [NullaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [NullaryPhysicalOperatorNode]
     */
    final override fun digest(): Digest = this.hashCode().toLong()
}