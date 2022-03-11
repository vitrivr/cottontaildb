package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.CountProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityCountPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
object CountPushdownRule : RewriteRule {
    /**
     * The [CountPushdownRule] can be applied to all [CountProjectionPhysicalOperatorNode]s that directly follow an [EntityScanPhysicalOperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [CountPushdownRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean
        = node is CountProjectionPhysicalOperatorNode && node.input is EntityScanPhysicalOperatorNode

    /**
     * Applies this [CountPushdownRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {
        require(node is CountProjectionPhysicalOperatorNode) { "Called CountPushdownRule.apply() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}

        /* Parse input. */
        val input = node.input
        require(input is EntityScanPhysicalOperatorNode) { "Called CountPushdownRule.apply() on a node that does not directly follow an entity scan. This is a programmer's error!" }

        /* Preform rewrite. */
        val p = EntityCountPhysicalOperatorNode(input.groupId, input.entity, node.out)
        return node.output?.copyWithOutput(p) ?: p
    }
}