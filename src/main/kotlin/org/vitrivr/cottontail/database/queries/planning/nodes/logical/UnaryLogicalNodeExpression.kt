package org.vitrivr.cottontail.database.queries.planning.nodes.logical

/**
 * An abstract [LogicalNodeExpression] implementation that has a single [NodeExpression] as input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class UnaryLogicalNodeExpression : LogicalNodeExpression() {
    /** Input arity of [UnaryLogicalNodeExpression] is always one. */
    final override val inputArity: Int = 1

    /** Reference to the input [LogicalNodeExpression]. */
    val input: LogicalNodeExpression
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is LogicalNodeExpression) {
                return input
            } else {
                throw Exception()
            }
        }
}