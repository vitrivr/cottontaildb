package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Decomposes a [FilterLogicalOperatorNode] that contains a [BooleanPredicate.Compound.And] into a sequence of two [FilterLogicalOperatorNode]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object LeftConjunctionRewriteRule : RewriteRule {

    /**
     * Checks if this [LeftConjunctionRewriteRule] can be applied to the given [OperatorNode].
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean =
        node is FilterLogicalOperatorNode && node.predicate is BooleanPredicate.Compound.And


    /**
     * Decomposes the provided [FilterLogicalOperatorNode] with a conjunction into two consecutive
     * [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode] covers
     * one part of the conjunction. Gives precedence to the left part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
        if (node is FilterLogicalOperatorNode && node.predicate is BooleanPredicate.Compound) {
            val parent = node.input?.copyWithInputs() ?: throw IllegalStateException("Encountered null node in logical operator node tree (node = $node). This is a programmer's error!")
            val ret = FilterLogicalOperatorNode(FilterLogicalOperatorNode(parent, node.predicate.p1), node.predicate.p2)
            return node.output?.copyWithOutput(ret) ?: ret
        }
        return null
    }
}