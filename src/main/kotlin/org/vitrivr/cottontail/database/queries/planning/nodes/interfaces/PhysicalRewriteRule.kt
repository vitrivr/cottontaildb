package org.vitrivr.cottontail.database.queries.planning.nodes.interfaces

/**
 * A rewrite rule for [LogicalNodeExpression] that replaces them with new [PhysicalNodeExpression]s.
 * Applied during query optimization.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface PhysicalRewriteRule : RewriteRule<NodeExpression.PhysicalNodeExpression> {
    /**
     * Checks if this [LogicalRewriteRule] can be applied to the given [NodeExpression]
     *
     * @param node The input [NodeExpression] to check.
     * @return True if [LogicalRewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: NodeExpression): Boolean

    /**
     * Transforms the given [NodeExpression] (and potentially its parents and children) to a new
     * [NodeExpression] that produces the equivalent output.
     *
     * @param node The input [NodeExpression].
     * @return The output [NodeExpression] or null, if no rewrite was done.
     */
    override fun apply(node: NodeExpression): NodeExpression.PhysicalNodeExpression?
}