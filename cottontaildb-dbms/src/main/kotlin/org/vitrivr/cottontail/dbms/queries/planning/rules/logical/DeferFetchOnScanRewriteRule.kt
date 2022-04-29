package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns scanned in an [EntityScanLogicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object DeferFetchOnScanRewriteRule: RewriteRule {
    /**
     * The [DeferFetchOnScanRewriteRule] can be applied to all [EntityScanLogicalOperatorNode]s.
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [DeferFetchOnScanRewriteRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is EntityScanLogicalOperatorNode

    /**
     * Apples this [DeferFetchOnScanRewriteRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext):OperatorNode? {
        /* Make sure, that node is a FetchLogicalOperatorNode. */
        require(node is EntityScanLogicalOperatorNode) { "Called DeferFetchOnFetchRewriteRule.rewrite() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}

        val candidates = node.fetch.map { it.first to it.second }.toMutableList()

        /* Check for early abort; if next node requires all candidates. */
        if (candidates.all { node.output?.requires?.contains(it.first.column) == true }) {
            return null
        }
        val originalGroupId = node.groupId
        var next: OperatorNode.Logical? = node.output

        while (next != null && next.groupId == originalGroupId) {
            /* Check if we encounter a node that requires specific but not all of the original columns. */
            val required = candidates.filter { it.first.column in next!!.requires }
            if (required.isEmpty()) {
                next = next.output
            } else {
                val defer = candidates.filter { it !in required }
                var p = next.copyWithInputs().base.first().output!!.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, required.map { it.first to it.second }))
                if (next.output != null) {
                    p = FetchLogicalOperatorNode(p, node.entity, defer.map { it.first to it.second })
                    p = next.output?.copyWithOutput(p) ?: p
                }
                return p
            }
        }

        /* This usually only happens for count(*) or exists (*) queries. */
        return node.output?.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, listOf(node.fetch.first())))
    }
}