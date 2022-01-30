package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.merge

import org.vitrivr.cottontail.dbms.queries.planning.nodes.OperatorNode
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that merges a [SortPhysicalOperatorNode] followed by a [LimitPhysicalOperatorNode]
 * into a [LimitingSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object LimitingSortMergeRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is LimitPhysicalOperatorNode && node.input is SortPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is LimitPhysicalOperatorNode) {
            val sort = node.input
            if (sort is SortPhysicalOperatorNode) {
                val input = sort.input?.copyWithInputs() ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $node). This is a programmer's error!")
                val p = LimitingSortPhysicalOperatorNode(input, sort.sortOn, node.limit, node.skip)
                return node.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }
}