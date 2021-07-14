package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns scanned in an [EntityScanLogicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
object DeferFetchOnScanRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is EntityScanLogicalOperatorNode
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is EntityScanLogicalOperatorNode) {

            val candidates = node.fetch.map { it.second.copy(it.first) to it.second }.toMutableList()

            /* Check for early abort; if next node requires all candidates. */
            if (candidates.all { node.output?.requires?.contains(it.first) == true }) {
                return null
            }
            val originalGroupId = node.groupId
            var next: OperatorNode.Logical? = node.output

            while (next != null && next.groupId == originalGroupId) {
                /* Check if we encounter a node that requires specific but not all of the original columns. */
                val required = candidates.filter { it.first in next!!.requires }
                if (required.isEmpty()) {
                    next = next.output
                } else {
                    val defer = candidates.filter { it !in required }
                    var p = next.copyWithInputs().base.first().output!!.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, required.map { it.first.name to it.second }))
                    if (next.output != null) {
                        p = FetchLogicalOperatorNode(p, node.entity, defer.map { it.first.name to it.second })
                        p = next.output?.copyWithOutput(p) ?: p
                    }
                    return p
                }
            }

            /* This should not happen because essentially, this means that no useful output is produced by query and hence no columns need to be fetched at all. */
            return node.output?.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, emptyList()))
        }
        return null
    }
}