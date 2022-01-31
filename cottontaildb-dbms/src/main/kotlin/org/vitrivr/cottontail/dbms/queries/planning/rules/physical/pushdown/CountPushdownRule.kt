package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.CountProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityCountPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
object CountPushdownRule : RewriteRule {
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean = node is CountProjectionPhysicalOperatorNode

    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
        if (node is CountProjectionPhysicalOperatorNode) {
            val input = node.input
            if (input is EntityScanPhysicalOperatorNode) {
                val p = EntityCountPhysicalOperatorNode(input.groupId, input.entity, node.out)
                return node.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }
}