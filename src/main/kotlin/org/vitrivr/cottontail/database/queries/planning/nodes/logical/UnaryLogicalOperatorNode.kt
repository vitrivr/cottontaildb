package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException

/**
 * An abstract [OperatorNode.Logical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class UnaryLogicalOperatorNode : OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 1

    /** Reference to the input [OperatorNode.Logical]. */
    val input: OperatorNode.Logical
        get() = when (val input = this.inputs.getOrNull(0)) {
            is Logical -> input
            else -> throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(this, "Invalid input for unary logical node expression.")
        }
}