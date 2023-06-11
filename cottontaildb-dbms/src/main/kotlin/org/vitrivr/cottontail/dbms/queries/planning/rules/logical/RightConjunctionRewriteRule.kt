package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Decomposes a [FilterLogicalOperatorNode] that contains a [BooleanPredicate.And]
 * into a sequence of two [FilterLogicalOperatorNode]s.
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
object RightConjunctionRewriteRule : RewriteRule {

    /**
     * Checks if this [RightConjunctionRewriteRule] can be applied to the given [OperatorNode].
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean =
        node is FilterLogicalOperatorNode && node.predicate is BooleanPredicate.And

    /**
     * Decomposes the provided [FilterLogicalOperatorNode] with a conjunction (AND) into two
     * consecutive [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode]
     * covers one part of the conjunction. Gives precedence to the right part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {
        /* Make sure, that node is a LeftConjunctionRewriteRule. */
        require(node is FilterLogicalOperatorNode) { "Called RightConjunctionRewriteRule.apply() with node of type ${node.javaClass.simpleName} that is not a FilterLogicalOperatorNode. This is a programmer's error!"}
        require(node.predicate is BooleanPredicate.And) { "Called RightConjunctionRewriteRule.apply() with node a predicate that is not a conjunction. This is a programmer's error!" }

        val parent = node.input.copyWithExistingInput()
        val ret = FilterLogicalOperatorNode(FilterLogicalOperatorNode(parent, node.predicate.p2), node.predicate.p1)
        return node.output?.copyWithOutput(ret) ?: ret
    }
}