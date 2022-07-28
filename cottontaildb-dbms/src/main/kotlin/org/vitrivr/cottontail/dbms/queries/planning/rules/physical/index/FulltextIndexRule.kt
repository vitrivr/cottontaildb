package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.functions.math.score.FulltextScore
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNodeUtilities
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
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
 * @version 1.3.1
 */
object FulltextIndexRule : RewriteRule {

    /**
     * Checks if this [FulltextIndexRule] can be applied to the given [OperatorNode].
     *
     * @param node [OperatorNode] to check.
     * @return True if [FulltextIndexRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode
        && node.function.function is FulltextScore
        && node.input is EntityScanPhysicalOperatorNode

    /**
     * Applies this [FulltextIndexRule], transforming the execution of a [FulltextScore] function by an index scan.
     *
     * @param node The [OperatorNode] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode] or null, if transformation was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        /* Make sure, that node is a FetchLogicalOperatorNode. */
        require(node is FunctionPhysicalOperatorNode) { "Called FulltextIndexRule.apply() with node of type ${node.javaClass.simpleName} that is not a FunctionPhysicalOperatorNode. This is a programmer's error!"}

        /* Parse function and different parameters. */
        require(node.function.function == FulltextScore) { "Called FulltextIndexRule.apply() with node that does not hold a FulltextScore function. This is a programmer's error!"}

        /* Parse entity scan. */
        val scan = node.input
        require(scan is EntityScanPhysicalOperatorNode) { "Called FulltextIndexRule.apply() with node that does not follow and EntityScanPhysicalOperatorNode. This is a programmer's error!"}

        val probingArgument = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
        val queryString = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
        val predicate = BooleanPredicate.Atomic(ComparisonOperator.Binary.Match(probingArgument, queryString), false)

        /* This rule does not heed index hints, because it can lead the planner to not produce a plan at all. */
        val candidate = scan.entity.listIndexes().map {
            scan.entity.indexForName(it).newTx(ctx)
        }.find {
            it.state != IndexState.DIRTY && it.canProcess(predicate)
        }

        with(MissingRecord) {
            with(ctx.bindings) {
                if (candidate != null) {
                    val produces = candidate.columnsFor(predicate)
                    val indexScan = IndexScanPhysicalOperatorNode(scan.groupId, candidate, predicate, listOf(Pair(node.out, produces[0])))
                    val fetch = FetchPhysicalOperatorNode(indexScan, scan.entity, scan.fetch.filter { !produces.contains(it.second) })
                    if (node.output == null) return fetch
                    return OperatorNodeUtilities.chainIf(fetch, node.output!!) {
                        when (it) {
                            is SortPhysicalOperatorNode -> it.traits[OrderTrait] != indexScan.traits[OrderTrait] /* SortPhysicalOperatorNode is only retained, if order is different from index order. */
                            is FilterPhysicalOperatorNode -> {
                                if (it.predicate is BooleanPredicate.Atomic && it.predicate.operator is ComparisonOperator.Binary.Greater && !it.predicate.not) {
                                    val op = it.predicate.operator as ComparisonOperator.Binary.Greater
                                    ((op.left is Binding.Column && (op.left as Binding.Column).column == indexScan.columns.first() && op.right.getValue() == DoubleValue.ZERO) ||
                                            (op.right is Binding.Column && (op.right as Binding.Column).column == indexScan.columns.first() && op.left.getValue() == DoubleValue.ZERO)).not()
                                } else {
                                    true
                                }
                            }
                            else -> true
                        }
                    }
                }
            }
        }
        return null
    }
}