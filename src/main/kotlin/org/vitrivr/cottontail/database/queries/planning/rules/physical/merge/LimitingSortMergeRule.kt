package org.vitrivr.cottontail.database.queries.planning.rules.physical.merge

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that merges a [SortPhysicalOperatorNode] followed by a [LimitPhysicalOperatorNode]
 * into a [LimitingSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LimitingSortMergeRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is LimitPhysicalOperatorNode && node.input is SortPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is LimitPhysicalOperatorNode) {
            val sort = node.input
            if (sort is SortPhysicalOperatorNode) {
                val input = sort.input.deepCopy()
                val output = node.copyOutput()
                val p = LimitingSortPhysicalOperatorNode(sort.order, node.limit, node.skip)
                p.addInput(input)
                output?.addInput(p)
                return p
            }
        }
        return null
    }
}