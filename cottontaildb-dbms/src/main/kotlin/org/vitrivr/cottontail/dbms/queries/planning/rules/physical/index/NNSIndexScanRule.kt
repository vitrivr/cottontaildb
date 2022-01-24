package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.queries.OperatorNode
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.core.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.core.functions.math.VectorDistance
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types

/**
 * A [RewriteRule] that replaces a very specific operator constellation that indicates a Nearest Neighbor Search (NNS)
 * by an index scan. The constellation can be described as follows:
 *
 * - Operators: Scan -> Function -> Sort by Distance -> Limit => NNS.
 * - The function needs to be a VectorDistance operating on a column (vector) and a literal (query)
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
object NNSIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is FunctionPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FunctionPhysicalOperatorNode && node.function is VectorDistance<*>) {
            val queryColumn = node.arguments.filterIsInstance<Binding.Column>().singleOrNull()?.column ?: return null
            val vectorLiteral = node.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
            val scan = node.input
            if (scan is EntityScanPhysicalOperatorNode) {
                val physicalQueryColumn = scan.fetch.singleOrNull { it.first == queryColumn.name }?.second ?: return null
                val distanceColumn = ColumnDef(Name.ColumnName(node.function.name.simple), Types.Double)
                val sort = node.output
                if (sort is SortPhysicalOperatorNode) {
                    if (sort.sortOn.first().first != node.columns.last()) return null /* Sort on distance column is required. */
                    val limit = sort.output
                    if (limit is LimitPhysicalOperatorNode) {
                        /* Column produced by the kNN. */
                        val predicate = KnnPredicate(physicalQueryColumn, limit.limit.toInt(), node.function, vectorLiteral)
                        val candidate = scan.entity.listIndexes().find { it.canProcess(predicate) }
                        if (candidate != null) {
                            when {
                                /* Case 1: Index produces distance, hence no distance calculation required! */
                                candidate.produces.contains(distanceColumn) -> {
                                    var p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, listOf(Pair(node.alias ?: distanceColumn.name, distanceColumn)))
                                    val newFetch = scan.fetch.filter { !candidate.produces.contains(it.second) && it != predicate.column }
                                    if (newFetch.isNotEmpty()) {
                                        p = FetchPhysicalOperatorNode(p, scan.entity, newFetch)
                                    }
                                    return sort.output?.copyWithOutput(p) ?: p /* TODO: Index should indicate if results are sorted. */
                                }

                                /* Case 2: Index produces the columns needed for the NNS operation. */
                                candidate.produces.contains(queryColumn) -> {
                                    val index = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, listOf(Pair(queryColumn.name, physicalQueryColumn)))
                                    var p: OperatorNode.Physical = FunctionPhysicalOperatorNode(index, node.function, node.arguments, node.alias)
                                    val newFetch = scan.fetch.filter { !candidate.produces.contains(it.second) }
                                    if (newFetch.isNotEmpty()) {
                                        p = FetchPhysicalOperatorNode(p, scan.entity,newFetch)
                                    }
                                    return node.output?.copyWithOutput(p) ?: p
                                }

                                /* Case 3: Index only produces TupleIds. Column for NNS needs to be fetched in an extra step. */
                                else -> {
                                    val index = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, emptyList())
                                    val distance = FunctionPhysicalOperatorNode(index, node.function, node.arguments, node.alias)
                                    val p = FetchPhysicalOperatorNode(distance, scan.entity, scan.fetch.filter { !candidate.produces.contains(it.second) })
                                    return node.output?.copyWithOutput(p) ?: p
                                }
                            }
                        }
                    }
                }
            }
        }
        return null
    }
}