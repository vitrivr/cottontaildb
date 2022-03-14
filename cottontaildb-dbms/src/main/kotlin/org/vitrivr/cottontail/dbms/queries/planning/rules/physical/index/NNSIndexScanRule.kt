package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.QueryHint
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
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
 * @version 1.4.0
 */
object NNSIndexScanRule : RewriteRule {

    /**
     * The [NNSIndexScanRule] can be applied to all [FunctionPhysicalOperatorNode]s of the [QueryHint.NoIndex] has not been set in the [QueryContext].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [NNSIndexScanRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode
        && node.function.function is VectorDistance<*>
        && node.input is EntityScanPhysicalOperatorNode
        && !ctx.hints.contains(QueryHint.NoIndex)

    /**
     * Applies this [NNSIndexScanRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        /* Make sure, that node is a FetchLogicalOperatorNode. */
        require(node is FunctionPhysicalOperatorNode) { "Called NNSIndexScanRule.apply() with node of type ${node.javaClass.simpleName} that is not a FunctionPhysicalOperatorNode. This is a programmer's error!"}

        /* Parse function and different parameters. */
        val function = node.function.function
        require(function is VectorDistance<*>) { "Called NNSIndexScanRule.apply() with node that does not hold a vector distance. This is a programmer's error!"}

        val queryColumn = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
        val vectorLiteral = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null

        /* Parse entity scan. */
        val scan = node.input
        require(scan is EntityScanPhysicalOperatorNode) { "Called NNSIndexScanRule.apply() with node that does not follow an EntityScanPhysicalOperatorNode. This is a programmer's error!"}

        val physicalQueryColumn = scan.fetch.singleOrNull { it.first == queryColumn }?.second ?: return null
        val sort = node.output
        if (sort is SortPhysicalOperatorNode) {
            if (sort.sortOn.first().first != node.columns.last()) return null /* Sort on distance column is required. */
            val limit = sort.output
            if (limit is LimitPhysicalOperatorNode) {
                /* Column produced by the kNN. */
                val predicate = ProximityPredicate.NNS(physicalQueryColumn, limit.limit.toInt(), function, vectorLiteral)
                val candidate = scan.entity.listIndexes().map {
                    scan.entity.indexForName(it)
                }.find {
                    it.state != IndexState.DIRTY && it.canProcess(predicate)
                }
                if (candidate != null) {
                    val produces = candidate.produces(predicate)
                    val distanceColumn = predicate.distanceColumn
                    when {
                        /* Case 1: Index produces distance, hence no distance calculation required! */
                        produces.contains(predicate.distanceColumn) -> {
                            var p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, listOf(Pair(node.out.copy(), distanceColumn)))
                            val newFetch = scan.fetch.filter { !produces.contains(it.second) && it != predicate.column }
                            if (newFetch.isNotEmpty()) {
                                p = FetchPhysicalOperatorNode(p, scan.entity, newFetch)
                            }
                            return limit.output?.copyWithOutput(p) ?: p /* TODO: Index should indicate if results are sorted. */
                        }

                        /* Case 2: Index produces the columns needed for the NNS operation. */
                        produces.contains(queryColumn.column) -> {
                            val index = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, listOf(Pair(queryColumn.copy(), physicalQueryColumn)))
                            var p: OperatorNode.Physical = FunctionPhysicalOperatorNode(index, node.function, node.out.copy())
                            val newFetch = scan.fetch.filter { !produces.contains(it.second) }
                            if (newFetch.isNotEmpty()) {
                                p = FetchPhysicalOperatorNode(p, scan.entity,newFetch)
                            }
                            return node.output?.copyWithOutput(p) ?: p
                        }

                        /* Case 3: Index only produces TupleIds. Column for NNS needs to be fetched in an extra step. */
                        else -> {
                            val index = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, emptyList())
                            val distance = FunctionPhysicalOperatorNode(index, node.function, node.out.copy())
                            val p = FetchPhysicalOperatorNode(distance, scan.entity, scan.fetch.filter { !produces.contains(it.second) })
                            return node.output?.copyWithOutput(p) ?: p
                        }
                    }
                }
            }
        }
        return null
    }
}