package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * An abstract [PhysicalOperatorNode] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class UnaryPhysicalOperatorNode : OperatorNode.Physical() {

    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    override val inputArity = 1

    /** [OperatorNode.Physical]s are executable if all their inputs are executable. */
    override val executable: Boolean
        get() = this.input.executable

    /** [UnaryPhysicalOperatorNode] can be partitioned, if its parent can be partitioned. */
    override val canBePartitioned: Boolean
        get() = this.input.canBePartitioned

    /**
     * Tries to create [p] partitions of this [UnaryPhysicalOperatorNode], which is possible
     * as long as all upstream [OperatorNode.Physical]s can be partitioned.
     *
     * @param p The desired number of partitions.
     * @return Array of [OperatorNode.Physical]s.
     *
     * @throws IllegalStateException If this [OperatorNode.Physical] cannot be partitioned.
     */
    override fun partition(p: Int): List<OperatorNode.Physical> = this.input.partition(p).map {
        val copy = this.copy()
        copy.addInput(it)
        copy
    }

    /** Reference to the input [OperatorNode.Physical]. */
    val input: OperatorNode.Physical
        get() = when (val input = this.inputs.getOrNull(0)) {
            is Physical -> input
            else -> throw IllegalArgumentException("Tried to access invalid input for unary physical node expression. This is a programmer's error.")
        }
}