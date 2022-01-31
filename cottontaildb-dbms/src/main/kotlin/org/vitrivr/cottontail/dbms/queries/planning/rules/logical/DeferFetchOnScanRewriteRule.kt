package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns scanned in an [EntityScanLogicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object DeferFetchOnScanRewriteRule : RewriteRule {
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean = node is EntityScanLogicalOperatorNode
    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
        if (node is EntityScanLogicalOperatorNode) {

            val candidates = node.fetch.map { it.first to it.second }.toMutableList()

            /* Check for early abort; if next node requires all candidates. */
            if (candidates.all { node.output?.requires?.contains(it.first.column) == true }) {
                return null
            }
            val originalGroupId = node.groupId
            var next: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical? = node.output

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

            /* This should not happen because essentially, this means that no useful output can be produced by query and hence no columns need to be fetched at all. */
            return node.output?.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, emptyList()))
        }
        return null
    }
}