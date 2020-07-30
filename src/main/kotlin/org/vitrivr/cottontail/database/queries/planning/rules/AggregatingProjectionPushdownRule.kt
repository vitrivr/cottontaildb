package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.EntityScanNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.ProjectionNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.pushdown.AggregatingProjectionPushdownNodeExpression

/**
 * This [NodeRewriteRule] merges a [ProjectionNodeExpression] with the preceding [EntityScanNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object AggregatingProjectionPushdownRule : NodeRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean =
            node is ProjectionNodeExpression
                    && node.type.aggregating
                    && node.parents.firstOrNull() is EntityScanNodeExpression.FullEntityScanNodeExpression

    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is ProjectionNodeExpression && node.type.aggregating) {
            val p1 = node.parents.first()
            if (p1 is EntityScanNodeExpression.FullEntityScanNodeExpression) {
                val column = node.columns.firstOrNull()
                val res = if (column != null) {
                    AggregatingProjectionPushdownNodeExpression(node.type, p1.entity, column, node.fields[column.name]?.name)
                } else {
                    AggregatingProjectionPushdownNodeExpression(node.type, p1.entity, null, null)
                }
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