package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
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
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.InMemorySortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that replaces the execution of a [FulltextScore] function by an index scan. Searches
 * and replaces a very specific constellation:
 *
 * - Operators: Scan -> Function => Lucene Search.
 * - Function: Executed function must be the [FulltextScore] function.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object FulltextIndexRule : RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [FulltextIndexRule], transforming the execution of a [FulltextScore] function by an index scan.
     *
     * @param node The [OperatorNode.Physical] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode.Physical] or null, if transformation was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        /* Extract necessary components. */
        if (node !is FunctionPhysicalOperatorNode) return null
        if (node.function.function != FulltextScore) return null
        val scan = node.input as? EntityScanPhysicalOperatorNode ?: return null

        /* Extract function arguments. */
        val probingArgument = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
        val queryString = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
        val predicate = BooleanPredicate.Comparison(ComparisonOperator.Match(probingArgument, queryString))

        /* This rule does not heed index hints, because it can lead the planner to not produce a plan at all. */
        val candidate = scan.tx.listIndexes().map {
            scan.tx.indexForName(it).newTx(scan.tx)
        }.find {
            it.state != IndexState.DIRTY && it.canProcess(predicate)
        }

        with(MissingTuple) {
            with(ctx.bindings) {
                if (candidate != null) {
                    val produces = candidate.columnsFor(predicate)
                    val indexScan = IndexScanPhysicalOperatorNode(scan.groupId, listOf(ctx.bindings.bind(node.out.column, produces[0])), candidate, predicate)
                    return OperatorNodeUtilities.chainIf(indexScan, node.output!!) {
                        when (it) {
                            is InMemorySortPhysicalOperatorNode -> it.traits[OrderTrait] != indexScan.traits[OrderTrait] /* SortPhysicalOperatorNode is only retained, if order is different from index order. */
                            is FilterPhysicalOperatorNode -> {
                                if (it.predicate is BooleanPredicate.Comparison && it.predicate.operator is ComparisonOperator.Greater) {
                                    val op = it.predicate.operator as ComparisonOperator.Greater
                                    ((op.left is Binding.Column && (op.left as Binding.Column) == indexScan.columns.first() && op.right.getValue() == DoubleValue.ZERO) ||
                                            (op.right is Binding.Column && (op.right as Binding.Column) == indexScan.columns.first() && op.left.getValue() == DoubleValue.ZERO)).not()
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