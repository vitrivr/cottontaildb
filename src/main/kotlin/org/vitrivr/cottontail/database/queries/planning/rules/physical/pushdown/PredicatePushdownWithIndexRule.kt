package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanFilterPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.IndexedFilterPhysicalNodeExpression

/**
 * This [PhysicalRewriteRule] replaces a [EntityScanFilterPhysicalNodeExpression] with the preceding
 * [IndexedFilterPhysicalNodeExpression], which usually leverages some sort of index to achieve the same result.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object PredicatePushdownWithIndexRule : PhysicalRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is EntityScanFilterPhysicalNodeExpression
    override fun apply(node: NodeExpression): PhysicalNodeExpression? {
        if (node is EntityScanFilterPhysicalNodeExpression) {
            val index = node.entity.allIndexes().find { it.canProcess(node.predicate) }
            if (index != null) {
                val res = IndexedFilterPhysicalNodeExpression(node.entity, index, node.predicate)
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