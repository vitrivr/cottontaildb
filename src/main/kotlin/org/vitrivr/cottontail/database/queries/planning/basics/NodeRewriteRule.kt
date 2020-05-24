package org.vitrivr.cottontail.database.queries.planning.basics

/**
 * A rewrite rule for [NodeExpression]. Applied during query optimization.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface NodeRewriteRule {
    /**
     * Checks, if this [NodeRewriteRule] can be applied to the given [NodeExpression].
     *
     * @param node [NodeExpression] to check.
     * @return true if this [NodeRewriteRule] can be applied, false otherwise.
     */
    fun canBeApplied(node: NodeExpression): Boolean

    /**
     * Transforms the given [NodeExpression] (and parent [NodeExpression]s) into an equivalent
     * [NodeExpression] using the given [NodeRewriteRule].
     */
    fun apply(node: NodeExpression): NodeExpression?
}