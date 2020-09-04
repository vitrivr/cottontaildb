package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.LogicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.PhysicalRewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanKnnPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity.EntityScanPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset.RecordsetKnnPhysicalNodeExpression

/**
 * This [LogicalRewriteRule] merges a [RecordsetKnnPhysicalNodeExpression] with the preceding [EntityScanPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
object KnnPushdownRule : PhysicalRewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean {
        if (node !is KnnLogicalNodeExpression) {
            return false
        }
        return when (val p = node.inputs.firstOrNull()) {
            is EntityScanLogicalNodeExpression.EntityFullScanLogicalNodeExpression -> true
            is FilterLogicalNodeExpression -> p.inputs.firstOrNull() is EntityScanLogicalNodeExpression.EntityFullScanLogicalNodeExpression
            else -> false
        }
    }

    override fun apply(node: NodeExpression): PhysicalNodeExpression? {
        if (node is KnnLogicalNodeExpression) {
            val res = when (val p = node.inputs.firstOrNull()) {
                is EntityScanLogicalNodeExpression.EntityFullScanLogicalNodeExpression -> EntityScanKnnPhysicalNodeExpression(p.entity, node.predicate)
                is FilterLogicalNodeExpression -> {
                    val pp = p.inputs.firstOrNull()
                    if (pp is EntityScanLogicalNodeExpression.EntityFullScanLogicalNodeExpression) {
                        EntityScanKnnPhysicalNodeExpression(pp.entity, node.predicate, p.predicate)
                    } else {
                        return null /* Returns from function call. */
                    }
                }
                else -> return null /* Returns from function call. */
            }
            val childNode = node.copyOutput()
            if (childNode != null) {
                res.updateOutput(childNode)
            }
            return res
        }
        return null
    }
}