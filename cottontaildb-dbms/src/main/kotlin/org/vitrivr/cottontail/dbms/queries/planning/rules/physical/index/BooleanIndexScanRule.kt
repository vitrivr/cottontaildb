package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
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
 * @version 2.0.0
 */
object BooleanIndexScanRule : RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [BooleanIndexScanRule] and tries to replace a [EntityScanPhysicalOperatorNode] followed by a [FilterLogicalOperatorNode]
     *
     * @param node The [OperatorNode.Physical] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode.Physical] or null, if transformation was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        if (ctx.hints.contains(QueryHint.IndexHint.None)) return null

        /* Extract necessary components. */
        val filter = node as? FilterPhysicalOperatorNode ?: return null
        val predicate = node.predicate as? BooleanPredicate ?: return null
        val parent = filter.input as? EntityScanPhysicalOperatorNode ?: return null

        /* Extract index hint and search for candidate. */
        val candidate = parent.tx.listIndexes().map {
            parent.tx.indexForName(it).newTx(parent.tx)
        }.find {
            it.state != IndexState.DIRTY && it.canProcess(predicate)
        }

        if (candidate != null) {
            val produced = candidate.columnsFor(node.predicate)
            val indexColumns = parent.columns.filter { produced.contains(it.physical!!) }
            val p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, indexColumns, candidate, node.predicate)
            return node.output?.copyWithOutput(p) ?: p
        }
        return null
    }
}