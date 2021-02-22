package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException

/**
 * An abstract [BinaryLogicalOperatorNode] implementation that has exactly two [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class BinaryLogicalOperatorNode : OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 2

    /** Reference to the input [LogicalOperatorNode]. */
    val input1: OperatorNode.Logical
        get() = when (val input = this.inputs.getOrNull(0)) {
            is Logical -> input
            else -> throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(this, "Tried to access invalid input 1 for binary logical node expression. This is a programmer's error.")
        }

    /** Reference to the input [LogicalOperatorNode]. */
    val input2: OperatorNode.Logical
        get() = when (val input = this.inputs.getOrNull(1)) {
            is Logical -> input
            else -> throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(this, "Tried to access invalid input 2 for binary logical node expression. This is a programmer's error.")
        }
}