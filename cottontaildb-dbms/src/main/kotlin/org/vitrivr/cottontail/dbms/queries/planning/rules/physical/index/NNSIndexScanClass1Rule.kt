package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that replaces a class 1 operator constellation related to proximity based queries by an index scan
 *
 * The constellation can be described as follows:
 * - Operators: Scan -> Function[VectorDistance]
 *
 * The function must be a VectorDistance operating on a column (vector) and a literal (query)
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
object NNSIndexScanClass1Rule : RewriteRule {
    /**
     * The [NNSIndexScanClass1Rule] can be applied to all [FunctionPhysicalOperatorNode]s that involve the execution of a [VectorDistance] if the
     * [QueryHint.IndexHint.None] has not been set in the [QueryContext].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [NNSIndexScanClass1Rule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode
            && node.function.function is VectorDistance<*>
            && node.input is EntityScanPhysicalOperatorNode
            && !ctx.hints.contains(QueryHint.IndexHint.None)

    /**
     * Applies this [NNSIndexScanClass3Rule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        /* Make sure, that node is a FetchLogicalOperatorNode. */
        require(node is FunctionPhysicalOperatorNode) {
            "Called NNSIndexScanRule.apply() with node of type ${node.javaClass.simpleName} that is not a FunctionPhysicalOperatorNode. This is a programmer's error!"
        }

        /* Parse function and different parameters. */
        val function = node.function.function
        require(function is VectorDistance<*>) {
            "Called NNSIndexScanClass1Rule.apply() with node that does not hold a vector distance. This is a programmer's error!"
        }

        /* Parse query column and vector literal. */
        val queryColumn = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
        val vectorLiteral = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null

        /* Parse entity scan and physical column. */
        val scan = node.input
        require(scan is EntityScanPhysicalOperatorNode) {
            "Called NNSIndexScanClass1Rule.apply() with node that does not follow an EntityScanPhysicalOperatorNode. This is a programmer's error!"
        }
        val physicalQueryColumn = scan.columns.singleOrNull { it == queryColumn } ?: return null

        /* Extract index hint and search for candidate. */
        val predicate = ProximityPredicate.Scan(physicalQueryColumn, node.out, function, vectorLiteral)
        val hint = ctx.hints.filterIsInstance<QueryHint.IndexHint>().firstOrNull() ?: QueryHint.IndexHint.All
        val candidate = scan.entity.listIndexes().map {
            scan.entity.indexForName(it).newTx(ctx)
        }.find {
            it.state != IndexState.DIRTY && hint.matches(it.dbo) && it.canProcess(predicate)
        }

        /* If candidate has been found, execute replacement. */
        if (candidate != null) {
            val produces = candidate.columnsFor(predicate)
            if (produces.contains(predicate.distanceColumn.column)) {
                var replacement: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, listOf(node.out.copy()), candidate, predicate)
                val newFetch = scan.columns.filter { !produces.contains(it.physical) }
                if (newFetch.isNotEmpty()) {
                    replacement = FetchPhysicalOperatorNode(replacement, scan.entity, newFetch)
                }
                return node.output?.copyWithOutput(replacement) ?: replacement
            }
        }
        return null
    }
}