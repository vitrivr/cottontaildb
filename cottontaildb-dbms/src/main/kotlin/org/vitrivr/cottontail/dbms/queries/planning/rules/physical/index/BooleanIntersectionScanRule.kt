package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexIntersectionScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import java.util.*

/**
 * A [RewriteRule] that replaces a [EntityScanLogicalOperatorNode] followed by [FilterLogicalOperatorNode] through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanIntersectionScanRule : RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [BooleanIntersectionScanRule] and tries to replace a [EntityScanPhysicalOperatorNode] followed by a [FilterLogicalOperatorNode]
     *
     * @param node The [OperatorNode.Physical] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode.Physical] or null, if transformation was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        if (ctx.hints.contains(QueryHint.IndexHint.None)) return null

        /* Extract necessary components. */
        val filter = node as? FilterPhysicalOperatorNode ?: return null
        val predicate = node.predicate as? BooleanPredicate.And ?: return null
        val parent = filter.input as? EntityScanPhysicalOperatorNode ?: return null

        /* Decompose predicate. */
        val list = LinkedList<BooleanPredicate.Comparison>()
        if (!this.decompose(predicate, list)) return null

        /* Now find indexes that can work with the individual predicates. */
        val indexes = parent.tx.listIndexes().map {
            parent.tx.indexForName(it).newTx(parent.tx)
        }.filter {
            it.state != IndexState.DIRTY
        }

        val matches = LinkedList<Pair<IndexTx, BooleanPredicate.Comparison>>()
        for (comparison in list) {
            val candidate = indexes.find { it.canProcess(comparison) }
            if (candidate == null)  return null
            matches.add(candidate to comparison)
        }

        /* Return new operator node. */
        val p: OperatorNode.Physical = IndexIntersectionScanPhysicalOperatorNode(node.groupId, parent.columns, matches)
        return node.output?.copyWithOutput(p) ?: p
    }

    /**
     * Decomposes a [BooleanPredicate.And] into a list of [BooleanPredicate.Comparison]. If the [BooleanPredicate.And] contains
     * other [BooleanPredicate.And], those are decomposed as well.
     *
     * @param predicate [BooleanPredicate.And] to decompose.
     * @param list [MutableList] to store the [BooleanPredicate.Comparison] in.
     * @return True if decomposition was successful, false otherwise.
     */
    private fun decompose(predicate: BooleanPredicate.And, list: MutableList<BooleanPredicate.Comparison>): Boolean {
        when (predicate.p1) {
            is BooleanPredicate.Comparison ->  list.add(predicate.p1 as BooleanPredicate.Comparison)
            is BooleanPredicate.And -> decompose(predicate.p1 as BooleanPredicate.And, list)
            else ->  return false
        }
        when (predicate.p2) {
            is BooleanPredicate.Comparison ->  list.add(predicate.p2 as BooleanPredicate.Comparison)
            is BooleanPredicate.And -> decompose(predicate.p2 as BooleanPredicate.And, list)
            else -> return false
        }
        return true
    }
}