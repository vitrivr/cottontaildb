package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.merge

import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that merges a [SortPhysicalOperatorNode] followed by a [LimitPhysicalOperatorNode]
 * into a [LimitingSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object LimitingSortMergeRule : RewriteRule {
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean = node is LimitPhysicalOperatorNode && node.input is SortPhysicalOperatorNode

    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
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