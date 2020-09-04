package org.vitrivr.cottontail.database.queries.planning.nodes.interfaces

/**
 * A rewrite rule for [LogicalNodeExpression] that replaces them with new [LogicalNodeExpression]s.
 * Applied during query optimization.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface LogicalRewriteRule : RewriteRule<NodeExpression.LogicalNodeExpression> {
    /**
     * Checks if this [LogicalRewriteRule] can be applied to the given [NodeExpression]
     *
     * @param node The input [NodeExpression] to check.
     * @return True if [LogicalRewriteRule] can be applied, false otherwise.
     */
    fun canBeApplied(node: NodeExpression.LogicalNodeExpression): Boolean

    /**
     * Transforms the given [NodeExpression] (and potentially its parents and children) to a new
     * [NodeExpression] that produces the equivalent output.
     *
     * @param node The input [NodeExpression].
     * @return The output [NodeExpression] or null, if no rewrite was done.
     */
    fun apply(node: NodeExpression.LogicalNodeExpression): NodeExpression.LogicalNodeExpression?
}