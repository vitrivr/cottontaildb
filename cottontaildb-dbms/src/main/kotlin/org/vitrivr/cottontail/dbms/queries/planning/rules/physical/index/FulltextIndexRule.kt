package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.dbms.functions.math.score.FulltextScore
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that replaces the execution of a [FulltextScore] function by an index scan. Searches
 * and replaces a very specific constellation:
 *
 * - Operators: Scan -> Function => Lucene Search.
 * - Function: Executed function must be the [FulltextScore] function.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object FulltextIndexRule : RewriteRule {

    /**
     * Checks if this [FulltextIndexRule] can be applied to the given [OperatorNode].
     *
     * @param node [OperatorNode] to check.
     * @return True if [FulltextIndexRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean
        = node is FunctionPhysicalOperatorNode && node.function.function is FulltextScore

    /**
     * Applies this [FulltextIndexRule], transforming the execution of a [FulltextScore] function by an index scan.
     *
     * @param node The [OperatorNode] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode] or null, if transformation was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FunctionPhysicalOperatorNode && node.function.function is FulltextScore) {
            val scan = node.input
            if (scan is EntityScanPhysicalOperatorNode) {
                val probingArgument = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
                val queryString = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
                val predicate = BooleanPredicate.Atomic(ComparisonOperator.Binary.Match(probingArgument, queryString), false, scan.groupId)
                val candidate = scan.entity.listIndexes().map {
                    scan.entity.indexForName(it)
                }.find {
                    it.state != IndexState.DIRTY && it.canProcess(predicate)
                }
                if (candidate != null) {
                    val produces = candidate.produces(predicate)
                    val indexScan = IndexScanPhysicalOperatorNode(scan.groupId, ctx.txn.getTx(candidate) as IndexTx, predicate, listOf(Pair(node.out, produces[0])))
                    val fetch = FetchPhysicalOperatorNode(indexScan, scan.entity, scan.fetch.filter { !produces.contains(it.second) })
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