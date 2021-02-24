package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns that are not required to satisfy a [FetchLogicalOperatorNode]
 * until after that [FetchLogicalOperatorNode] has been executed.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferredFetchRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is EntityScanLogicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is EntityScanLogicalOperatorNode) {
            var next = node.output
            while (next != null) {
                /* when (next) {
                    is FilterLogicalOperatorNode,
                    is LimitLogicalOperatorNode -> {
                        val fetch = mutableListOf<ColumnDef<*>>()
                        val defer = mutableListOf<ColumnDef<*>>()
                        for (c in node.columns) {
                            if (next.requires.contains(c)) {
                                fetch.add(c)
                            } else {
                                defer.add(c)
                            }
                        }

                        /* Rewrite tree up and until this node. */
                        if (defer.size > 0) {
                            var scan = node.output!!
                            val base = scan.copyWithInputs()
                            var copy = base
                            while (scan != next) {
                                scan = scan.output!!
                                localCopy.addInput(copy)
                                copy = localCopy
                            }
                            val root = EntityScanLogicalOperatorNode(node.entity, fetch.toTypedArray())
                            base.addInput(root)
                            val fetchOp = FetchLogicalOperatorNode(node.entity, defer.toTypedArray())
                            fetchOp.addInput(copy)
                            next.copyOutput()?.addInput(fetchOp)
                            return root
                        }
                    }
                }*/
                next = next.output
            }
        }
        return null
    }
}