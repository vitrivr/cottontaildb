package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.LogicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanKnnPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.IndexedKnnPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.predicates.KnnPredicateHint

/**
 * This [LogicalRewriteRule] replaces a [EntityScanKnnPhysicalNodeExpression] with a [IndexedKnnPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnIndexRule : PhysicalRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = (node is EntityScanKnnPhysicalNodeExpression) && node.predicate == null && node.knn.hint != KnnPredicateHint.KnnNoIndexPredicateHint
    override fun apply(node: NodeExpression): PhysicalNodeExpression? {
        if (node is EntityScanKnnPhysicalNodeExpression && node.knn.hint != KnnPredicateHint.KnnNoIndexPredicateHint) {
            val index = node.entity.allIndexes().filter {
                it.canProcess(node.knn) && when (node.knn.hint) {
                    is KnnPredicateHint.KnnInexactPredicateHint -> true
                    is KnnPredicateHint.KnnIndexTypePredicateHint -> it.type == node.knn.hint.type
                    is KnnPredicateHint.KnnIndexNamePredicateHint -> it.name == node.knn.hint.name
                    else -> !it.type.inexact
                }
            }.minByOrNull {
                it.cost(node.knn)
            }

            if (index != null) {
                val res = IndexedKnnPhysicalNodeExpression(node.entity, node.knn, index)
                val childNode = node.copyOutput()
                if (childNode != null) {
                    res.updateOutput(childNode)
                }
                return res
            }
        }
        return null
    }
}