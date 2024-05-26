package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.sort

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.InMemorySortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that merges a [InMemorySortPhysicalOperatorNode] followed by a [LimitPhysicalOperatorNode]
 * into a [LimitingSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object LimitingSortMergeRule : RewriteRule<OperatorNode.Physical> {
    /**
     * Apples this [LimitingSortPhysicalOperatorNode] to the provided [OperatorNode], creating a new version of the tree.
     *
     * @param node The [OperatorNode.Physical] to apply this [LimitingSortPhysicalOperatorNode].
     * @param ctx The [QueryContext] used for planning.
     * @return Transformed [OperatorNode.Physical] or null, if transformation was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        /* Make sure, that node is a valid LimitPhysicalOperatorNode. */
        if (node !is LimitPhysicalOperatorNode) return null
        if (node.limit < Int.MAX_VALUE.toLong()) return null

        /* Parse sort node. */
        val sort = node.input as? InMemorySortPhysicalOperatorNode ?: return null

        /* Perform rewrite. */
        val input = sort.input.copyWithExistingInput()
        val p = LimitingSortPhysicalOperatorNode(input, sort.sortOn, node.limit.toInt())
        return node.output?.copyWithOutput(p) ?: p
    }
}