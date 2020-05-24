package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.EntityScanNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.FilterNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.pushdown.PredicatePushdownNodeExpression

/**
 * This [NodeRewriteRule] merges a [FilterNodeExpression] with the preceding [EntityScanNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object PredicatePushdownRule : NodeRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is FilterNodeExpression && node.parents.firstOrNull() is EntityScanNodeExpression.FullEntityScanNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FilterNodeExpression) {
            val p1 = node.parents.first()
            if (p1 is EntityScanNodeExpression.FullEntityScanNodeExpression) {
                val res = PredicatePushdownNodeExpression(p1.entity, node.predicate)
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