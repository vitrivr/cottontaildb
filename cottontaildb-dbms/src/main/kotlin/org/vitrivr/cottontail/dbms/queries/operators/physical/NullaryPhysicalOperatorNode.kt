package org.vitrivr.cottontail.dbms.queries.operators.physical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder

/**
 * An abstract [OperatorNode.Physical] implementation that has no input node, i.e., acts as a source.
 *
 * @author Ralph Gasser
 * @version 2.5.0
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

    /** By default, a [NullaryPhysicalOperatorNode] has no specific order. */
    override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>
        get() = emptyList()

    /** By default, a [NullaryPhysicalOperatorNode] does not have specific requirements. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /** By default, a [NullaryPhysicalOperatorNode] does not support partitioning. */
    override val canBePartitioned: Boolean
        get() = false

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NullaryPhysicalOperatorNode].
     */
    abstract override fun copy(): NullaryPhysicalOperatorNode

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
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] with its output reaching down to the [root] of the tree.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must be empty!
     * @return Copy of this [NullaryPhysicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Physical): Physical {
        require(input.isEmpty()) { "Cannot provide input for NullaryPhysicalOperatorNode." }
        val copy = this.copy()
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * By default, a [NullaryPhysicalOperatorNode] cannot be partitioned.
     *
     * Must be overridden in order to support partitioning.
     */
    override fun tryPartition(partitions: Int, p: Int? ): Physical? = null

    /**
     * Calculates and returns the digest for this [NullaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [NullaryPhysicalOperatorNode]
     */
    final override fun digest(): Digest = this.hashCode().toLong()
}