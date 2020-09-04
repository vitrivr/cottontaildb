package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.ProjectionLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.pushdown.AggregatingProjectionPushdownNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.RecordsetProjectionPhysicalNodeExpression

/**
 * This [PhysicalRewriteRule] merges a [ProjectionLogicalNodeExpression] with the preceding
 * [EntityScanLogicalNodeExpression] and thereby creates a [AggregatingProjectionPushdownNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
object AggregatingProjectionPushdownRule : PhysicalRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean =
            node is ProjectionLogicalNodeExpression
                    && node.type.aggregating
                    && node.inputs.firstOrNull() is EntityScanLogicalNodeExpression

    override fun apply(node: NodeExpression): PhysicalNodeExpression? {
        if (node is RecordsetProjectionPhysicalNodeExpression && node.type.aggregating) {
            val p1 = node.inputs.first()
            if (p1 is EntityScanLogicalNodeExpression) {
                val field = node.fields.firstOrNull()
                val res = if (field != null) {
                    AggregatingProjectionPushdownNodeExpression(node.type, p1.entity, field.first, field.second)
                } else {
                    AggregatingProjectionPushdownNodeExpression(node.type, p1.entity, null, null)
                }
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