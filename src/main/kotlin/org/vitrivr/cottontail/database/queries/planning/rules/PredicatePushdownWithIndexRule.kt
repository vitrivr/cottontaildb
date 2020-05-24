package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.pushdown.PredicatePushdownNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.pushdown.PredicatePushdownWithIndexNodeExpression

/**
 * This [NodeRewriteRule] replaces a [PredicatePushdownNodeExpression] with the preceding
 * [PredicatePushdownWithIndexNodeExpression], which usually leverages some sort of index to achieve the same result.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object PredicatePushdownWithIndexRule : NodeRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is PredicatePushdownNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is PredicatePushdownNodeExpression) {
            val index = node.entity.allIndexes().find { it.canProcess(node.predicate) }
            if (index != null) {
                val res = PredicatePushdownWithIndexNodeExpression(node.entity, index, node.predicate)
                val childNode = node.copyChildren()
                if (childNode != null) {
                    res.setChild(childNode)
                }
                return res
            }
        }
        return null
    }
}