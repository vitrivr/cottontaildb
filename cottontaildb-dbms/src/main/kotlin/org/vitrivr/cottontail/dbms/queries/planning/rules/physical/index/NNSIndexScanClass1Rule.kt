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
 * @version 2.0.0
 */
object NNSIndexScanClass1Rule : RewriteRule<OperatorNode.Physical> {
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

        /* Parse query column and vector literal. */
        val queryColumn = node.function.arguments.filterIsInstance<Binding.Column>().singleOrNull() ?: return null
        val vectorLiteral = node.function.arguments.filterIsInstance<Binding.Literal>().singleOrNull() ?: return null
        val physicalQueryColumn = scan.columns.singleOrNull { it == queryColumn } ?: return null

        /* Extract index hint and search for candidate. */
        val predicate = ProximityPredicate.Scan(physicalQueryColumn, node.out, function, vectorLiteral)
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
                val replacement: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, listOf(node.out.copy()), candidate, predicate)
                return node.output?.copyWithOutput(replacement) ?: replacement
            }
        }
        return null
    }
}