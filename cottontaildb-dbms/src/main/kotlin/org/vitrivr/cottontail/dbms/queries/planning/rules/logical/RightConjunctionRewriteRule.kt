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
 * @version 2.0.0
 */
object RightConjunctionRewriteRule : RewriteRule<OperatorNode.Logical> {
    /**
     * Decomposes the provided [FilterLogicalOperatorNode] with a conjunction (AND) into two
     * consecutive [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode]
     * covers one part of the conjunction. Gives precedence to the right part of the conjunction.
     *
     * @param node The input [OperatorNode.Logical].
     * @param ctx The [QueryContext] in which query planning takes place.
     * @return The output [OperatorNode.Logical] or null, if no rewrite was done.
     */
    override fun tryApply(node: OperatorNode.Logical, ctx: QueryContext): OperatorNode.Logical? {
        /* Make sure, that node is a FilterLogicalOperatorNode. */
        if (node !is FilterLogicalOperatorNode) return null

        /* Extract necessary components. */
        val predicate = node.predicate as? BooleanPredicate.And ?: return null
        val parent = node.input.copyWithExistingInput()

        /* Return transformed node. */
        val ret = FilterLogicalOperatorNode(FilterLogicalOperatorNode(parent, predicate.p2), predicate.p1)
        return node.output?.copyWithOutput(ret) ?: ret
    }
}