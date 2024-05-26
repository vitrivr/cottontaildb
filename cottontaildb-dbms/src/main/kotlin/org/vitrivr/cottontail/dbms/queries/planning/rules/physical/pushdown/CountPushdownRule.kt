package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.CountProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityCountPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object CountPushdownRule : RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [CountPushdownRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode.Physical] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode.Physical] or null, if rewrite was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        if (node !is CountProjectionPhysicalOperatorNode) return null

        /* Parse input. */
        val input = node.input as? EntityScanPhysicalOperatorNode ?: return null

        /* Preform rewrite. */
        val p = EntityCountPhysicalOperatorNode(input.groupId, input.tx, node.out)
        return node.output?.copyWithOutput(p) ?: p
    }
}