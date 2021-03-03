package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import java.util.*

/**
 * A [RewriteRule] that defers fetching of columns that are not required.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferredFetchRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is EntityScanLogicalOperatorNode
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is EntityScanLogicalOperatorNode) {
            val originalNodes = node.columns
            val originalGroupId = node.groupId
            val defer = LinkedList<ColumnDef<*>>()
            val fetch = LinkedList<ColumnDef<*>>()
            var next: OperatorNode.Logical? = node
            while (next != null && next.groupId == originalGroupId) {
                /* Ignore if node has no special requirements; determine which nodes to fetch right away and which to defer.*/
                if (next.output?.requires?.isNotEmpty() == true) {

                    for (n in originalNodes) {
                        if (n in next.output!!.requires) {
                            fetch.add(n)
                        } else {
                            defer.add(n)
                        }
                    }

                    /* If columns can be deferred, then forward. */
                    if (defer.isNotEmpty()) {
                        /*
                         * This is a very convoluted way of saying: We copy the tree starting from this node upwards
                         * and replace the (source) operator.
                         */
                        var p = next.output!!.copyWithInputs().base.first().output!!.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, fetch.toTypedArray()))
                        p = FetchLogicalOperatorNode(p, node.entity, defer.toTypedArray())
                        p = next.output?.output?.copyWithOutput(p) ?: p
                        return p
                    }
                }

                /* Move down the tree. */
                next = next.output
                defer.clear()
                fetch.clear()
            }
        }
        return null
    }
}