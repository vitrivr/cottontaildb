package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanFilterPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.RecordsetFilterPhysicalNodeExpression

/**
 * This [PhysicalRewriteRule] merges a [RecordsetFilterPhysicalNodeExpression] with the preceding [EntityScanPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
object PredicatePushdownRule : PhysicalRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is RecordsetFilterPhysicalNodeExpression && node.input is EntityScanPhysicalNodeExpression.FullEntityScanPhysicalNodeExpression
    override fun apply(node: NodeExpression): PhysicalNodeExpression? {
        if (node is RecordsetFilterPhysicalNodeExpression) {
            /* Merge predicate & entity scan. */
            val p1 = node.input
            if (p1 is EntityScanPhysicalNodeExpression.FullEntityScanPhysicalNodeExpression) {
                val res = EntityScanFilterPhysicalNodeExpression(p1.entity, node.predicate)
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