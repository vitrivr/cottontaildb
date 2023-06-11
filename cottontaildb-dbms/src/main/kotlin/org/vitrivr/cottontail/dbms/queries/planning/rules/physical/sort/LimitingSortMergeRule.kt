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
 * @version 1.3.0
 */
object LimitingSortMergeRule : RewriteRule {
    /**
     * The [LimitingSortMergeRule] can be applied to all [LimitPhysicalOperatorNode]s that directly follow [InMemorySortPhysicalOperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [LimitingSortMergeRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean
        = node is LimitPhysicalOperatorNode && node.limit < Int.MAX_VALUE.toLong() && node.input is InMemorySortPhysicalOperatorNode

    /**
     * Apples this [LimitingSortPhysicalOperatorNode] to the provided [OperatorNode], creating a new version of the tree.
     *
     * @param node The [OperatorNode] to apply this [LimitingSortPhysicalOperatorNode].
     * @param ctx The [QueryContext] used for planning.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {
        /* Make sure, that node is a LimitPhysicalOperatorNode. */
        require(node is LimitPhysicalOperatorNode) { "Called LimitingSortMergeRule.apply() with node of type ${node.javaClass.simpleName} that is not a LimitPhysicalOperatorNode. This is a programmer's error!"}
        require(node.limit < Int.MAX_VALUE.toLong()) { "Called LimitingSortMergeRule.apply() with a limit that exceeds ${Int.MAX_VALUE}. This is a programmer's error!"}

        /* Parse sort node. */
        val sort = node.input
        require(sort is InMemorySortPhysicalOperatorNode ) { "Called LimitingSortMergeRule.apply() with with node that does not follow a InMemorySortPhysicalOperatorNode." }

        /* Perform rewrite. */
        val input = sort.input.copyWithExistingInput()
        val p = LimitingSortPhysicalOperatorNode(input, sort.sortOn, node.limit.toInt())
        return node.output?.copyWithOutput(p) ?: p
    }
}