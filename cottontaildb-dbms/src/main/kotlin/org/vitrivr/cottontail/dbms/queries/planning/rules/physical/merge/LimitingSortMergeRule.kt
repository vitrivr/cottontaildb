package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.merge

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown.CountPushdownRule

/**
 * A [RewriteRule] that merges a [SortPhysicalOperatorNode] followed by a [LimitPhysicalOperatorNode]
 * into a [LimitingSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
object LimitingSortMergeRule : RewriteRule {
    /**
     * The [LimitingSortMergeRule] can be applied to all [LimitPhysicalOperatorNode]s that directly follow [SortPhysicalOperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [CountPushdownRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean
        = node is LimitPhysicalOperatorNode && node.input is SortPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {
        /* Make sure, that node is a LimitPhysicalOperatorNode. */
        require(node is LimitPhysicalOperatorNode) { "Called LimitingSortMergeRule.apply() with node of type ${node.javaClass.simpleName} that is not a LimitPhysicalOperatorNode. This is a programmer's error!"}

        /* Parse sort node. */
        val sort = node.input
        require(sort is SortPhysicalOperatorNode) { "Called LimitingSortMergeRule.apply() with with node that does not follow a SortPhysicalOperatorNode." }

        /* Perform rewrite. */
        val input = sort.input.copyWithExistingInput()
        val p = LimitingSortPhysicalOperatorNode(input, sort.sortOn, node.limit)
        return node.output?.copyWithOutput(p) ?: p
    }
}