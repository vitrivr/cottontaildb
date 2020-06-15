package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.index.KnnIndexNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.pushdown.KnnPushdownNodeExpression

/**
 * This [NodeRewriteRule] replaces a [KnnPushdownNodeExpression] with the preceding [KnnIndexNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnIndexRule: NodeRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = (node is KnnPushdownNodeExpression) && node.predicate == null
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is KnnPushdownNodeExpression) {
            val index = node.entity.allIndexes().filter {
                it.canProcess(node.knn) && (node.knn.inexact || !it.type.inexact)
            }.minBy {
                it.cost(node.knn)
            }

            if (index != null) {
                val res = KnnIndexNodeExpression(node.entity, node.knn, index)
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