package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform

import org.vitrivr.cottontail.core.recordset.PlaceholderRecord
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns scanned in an [EntityScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
object DeferFetchOnScanRewriteRule: RewriteRule {
    /**
     * The [DeferFetchOnScanRewriteRule] can be applied to all [EntityScanPhysicalOperatorNode]s.
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [DeferFetchOnScanRewriteRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is EntityScanPhysicalOperatorNode

    /**
     * Apples this [DeferFetchOnScanRewriteRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        with(ctx.bindings) {
            with(PlaceholderRecord) {
                /* Make sure, that node is a FetchLogicalOperatorNode. */
                require(node is EntityScanPhysicalOperatorNode) { "Called DeferFetchOnFetchRewriteRule.rewrite() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}

                val candidates = node.fetch.toMutableList()

                /* Check for early abort; if next node requires all candidates. */
                val originalGroupId = node.groupId
                var prev: OperatorNode.Physical? = node
                var next: OperatorNode.Physical? = node.output

                while (next != null && next.groupId == originalGroupId) {
                    /* Check if we encounter a node that requires specific but not all of the original columns. */
                    val required = candidates.filter { it.first.column in next!!.requires }
                    if (required.isNotEmpty()) {
                        /* Remove required elements from candidate list. */
                        candidates.removeAll(required)
                        if (candidates.isEmpty()) return null
                    }

                    /* Defer if end of tree is reached or expected number of output elements decreases. */
                    if (next.outputSize < prev!!.outputSize) {
                        if (candidates.size == node.fetch.size) {
                            candidates.removeFirst()
                        }
                        var p = next.copyWithExistingInput().base.first().output!!.copyWithOutput(EntityScanPhysicalOperatorNode(originalGroupId, node.entity, node.fetch.filter { !candidates.contains(it) })).root
                        if (next.output != null) {
                            p = FetchPhysicalOperatorNode(p, node.entity, candidates.map { it.first to it.second })
                            p = next.output?.copyWithOutput(p) ?: p
                        }
                        return p
                    }

                    /* Move to next nodes. */
                    prev = next
                    next = next.output
                }
                /* This usually only happens for count(*) or exists (*) queries. */
                return prev!!.copyWithExistingInput().base.first().output!!.copyWithOutput(EntityScanPhysicalOperatorNode(originalGroupId, node.entity, node.fetch.filter { !candidates.contains(it) }))
            }
        }
    }
}