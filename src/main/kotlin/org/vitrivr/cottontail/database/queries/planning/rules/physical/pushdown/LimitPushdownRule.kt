package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.LogicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LimitLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.RecordsetLimitPhysicalNodeExpression

/**
 * This [LogicalRewriteRule] merges a [RecordsetLimitPhysicalNodeExpression] with the preceding [EntityScanPhysicalNodeExpression.FullEntityScanPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object LimitPushdownRule : PhysicalRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean =
            (node is LimitLogicalNodeExpression)
                    && node.inputs.firstOrNull() is EntityScanPhysicalNodeExpression.FullEntityScanPhysicalNodeExpression

    override fun apply(node: NodeExpression): PhysicalNodeExpression? {
        if (node is RecordsetLimitPhysicalNodeExpression) {
            val p1 = node.inputs.first()
            if (p1 is EntityScanPhysicalNodeExpression.FullEntityScanPhysicalNodeExpression) {
                val start = 1L + node.skip // TODO: This actually only works if there are no deletions.
                val end = (start + node.limit).coerceAtMost(p1.entity.statistics.rows)
                val res = EntityScanPhysicalNodeExpression.RangedEntityScanPhysicalNodeExpression(p1.entity, p1.columns, start, end)
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