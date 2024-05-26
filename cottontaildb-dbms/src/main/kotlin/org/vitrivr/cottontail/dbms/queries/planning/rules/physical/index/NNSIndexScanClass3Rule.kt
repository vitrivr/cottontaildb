package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.InMemorySortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that replaces a very specific operator constellation that indicates a Nearest Neighbor Search (NNS)
 * by an index scan. The constellation can be described as follows:
 *
 * - Operators: Scan -> Function -> Sort by Distance -> Limit => NNS.
 * - The function needs to be a VectorDistance operating on a column (vector) and a literal (query)
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object NNSIndexScanClass3Rule : RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [NNSIndexScanClass3Rule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode.Physical] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode.Physical] or null, if rewrite was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        /* Extract necessary components. */
        if (node !is FunctionPhysicalOperatorNode) return null
        val function = node.function.function as? VectorDistance<*> ?: return null
        val scan = node.input as? EntityScanPhysicalOperatorNode ?: return null
        val sort = node.output as? InMemorySortPhysicalOperatorNode ?: return null
        val limit = sort.output as? LimitPhysicalOperatorNode ?: return null
        if (sort.sortOn.first().first != node.out) return null /* Sort on distance column is required. */

        /* Parse query column and vector literal. */
        val queryColumn = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
        val vectorLiteral = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
        val physicalQueryColumn = scan.columns.singleOrNull { it == queryColumn } ?: return null

        /* Column produced by the kNN. */
        val predicate = if (sort.sortOn.first().second == SortOrder.ASCENDING) {
            ProximityPredicate.NNS(physicalQueryColumn, node.out, limit.limit, function, vectorLiteral)
        } else {
            ProximityPredicate.FNS(physicalQueryColumn, node.out, limit.limit, function, vectorLiteral)
        }

        /* Extract index hint and search for candidate. */
        val hint = ctx.hints.filterIsInstance<QueryHint.IndexHint>().firstOrNull() ?: QueryHint.IndexHint.All
        val candidate = scan.tx.listIndexes().map {
            scan.tx.indexForName(it).newTx(scan.tx)
        }.find {
            it.state != IndexState.DIRTY && hint.matches(it.dbo) && it.canProcess(predicate)
        }

        /* If candidate has been found, execute replacement. */
        if (candidate != null) {
            val produces = candidate.columnsFor(predicate)
            if (produces.contains(predicate.distanceColumn.column)) {
                val fetch = mutableListOf(node.out.copy())
                if (produces.contains(predicate.column.physical)) {
                    fetch.add(scan.columns.filter { it == predicate.column }.map { it.copy() }.single())
                }
                val p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, fetch, candidate, predicate)
                return limit.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }
}