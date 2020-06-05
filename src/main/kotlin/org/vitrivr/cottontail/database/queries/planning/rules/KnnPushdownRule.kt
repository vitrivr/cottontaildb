package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.planning.basics.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.basics.NodeRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.EntityScanNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.KnnNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.basics.PredicatedKnnNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.pushdown.KnnPushdownNodeExpression

/**
 * This [NodeRewriteRule] merges a [KnnNodeExpression] with the preceding [EntityScanNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnPushdownRule : NodeRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = (node is KnnNodeExpression || node is PredicatedKnnNodeExpression) && node.parents.firstOrNull() is EntityScanNodeExpression.FullEntityScanNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is KnnNodeExpression || node is PredicatedKnnNodeExpression) {
            val p1 = node.parents.first()
            if (p1 is EntityScanNodeExpression.FullEntityScanNodeExpression) {
                val res = if (node is KnnNodeExpression) {
                    KnnPushdownNodeExpression(p1.entity, node.knn)
                } else {
                    KnnPushdownNodeExpression(p1.entity, (node as PredicatedKnnNodeExpression).knn, node.predicate)
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