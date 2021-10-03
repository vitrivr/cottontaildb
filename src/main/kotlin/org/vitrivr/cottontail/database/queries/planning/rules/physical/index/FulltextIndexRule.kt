package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FunctionProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.functions.math.score.FulltextScore

/**
 * A [RewriteRule] that replaces the execution of a [FulltextScore] function by an index scan. Searches
 * and replaces a very specific constellation:
 *
 * - Operators: Scan -> Function => Lucene Search.
 * - Function: Executed function must be the [FulltextScore] function.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FulltextIndexRule : RewriteRule {

    /**
     * Checks if this [FulltextIndexRule] can be applied to the given [OperatorNode].
     *
     * @param node [OperatorNode] to check.
     * @return True if [FulltextIndexRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: OperatorNode): Boolean
        = node is FunctionProjectionPhysicalOperatorNode && node.function is FulltextScore

    /**
     * Applies this [FulltextIndexRule], transforming the execution of a [FulltextScore] function by an index scan.
     *
     * @param node The [OperatorNode] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode] or null, if transformation was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FunctionProjectionPhysicalOperatorNode && node.function is FulltextScore) {
            val scan = node.input
            if (scan is EntityScanPhysicalOperatorNode) {
                val probingArgument = node.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
                val queryString = node.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
                val predicate = BooleanPredicate.Atomic(ComparisonOperator.Binary.Match(probingArgument, queryString), false, scan.groupId)
                val candidate = scan.entity.listIndexes().find { it.canProcess(predicate) }
                if (candidate != null) {
                    val indexScan = IndexScanPhysicalOperatorNode(scan.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, listOf(Pair(node.produces.name, candidate.produces[0])))
                    val fetch = FetchPhysicalOperatorNode(indexScan, scan.entity, scan.fetch.filter { !candidate.produces.contains(it.second) })
                    return if (node.output != null) {
                        node.output?.copyWithOutput(fetch)
                    } else {
                        fetch
                    }
                }
            }
        }
        return null
    }
}