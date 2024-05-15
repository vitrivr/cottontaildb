package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] preceded by a  [EntityScanLogicalOperatorNode]
 * through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean
        = !ctx.hints.contains(QueryHint.IndexHint.None) && node is FilterPhysicalOperatorNode && node.input is EntityScanPhysicalOperatorNode

    /**
     * Applies this [BooleanIndexScanRule] and tries to replace a [EntityScanPhysicalOperatorNode] followed by a [FilterLogicalOperatorNode]
     *
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (ctx.hints.contains(QueryHint.IndexHint.None)) return null
        if (node is FilterPhysicalOperatorNode) {
            val parent = node.input
            if (parent is EntityScanPhysicalOperatorNode) {
                /* Extract index hint and search for candidate. */
                val candidate = parent.tx.listIndexes().map {
                    parent.tx.indexForName(it).newTx(parent.tx)
                }.find {
                    it.state != IndexState.DIRTY && it.canProcess(node.predicate)
                }

                if (candidate != null) {
                    val produced = candidate.columnsFor(node.predicate)
                    val indexColumns = parent.columns.filter { produced.contains(it.physical!!) }
                    val p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, indexColumns, candidate, node.predicate)
                    return node.output?.copyWithOutput(p) ?: p
                }
            }
        }
        return null
    }
}