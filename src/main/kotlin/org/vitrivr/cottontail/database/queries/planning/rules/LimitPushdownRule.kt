package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.EntityScanNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.LimitNodeExpression

/**
 * This [NodeRewriteRule] merges a [LimitNodeExpression] with the preceding [EntityScanNodeExpression.FullEntityScanNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object LimitPushdownRule : NodeRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = (node is LimitNodeExpression) && node.parents.firstOrNull() is EntityScanNodeExpression.FullEntityScanNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is LimitNodeExpression) {
            val p1 = node.parents.first()
            if (p1 is EntityScanNodeExpression.FullEntityScanNodeExpression) {
                val start = 1L + node.skip // TODO: This actually only works if there are no deletions.
                val end = (start + node.limit)
                val res = EntityScanNodeExpression.RangedEntityScanNodeExpression(p1.entity, p1.columns, start, end)
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