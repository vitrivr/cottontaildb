package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] preceded by a  [EntityScanLogicalOperatorNode]
 * through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FilterPhysicalOperatorNode &&
        node.input is EntityScanPhysicalOperatorNode

    /**
     * Applies this [BooleanIndexScanRule] and tries to replace a [EntityScanPhysicalOperatorNode] followed by a [FilterLogicalOperatorNode]
     *
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FilterPhysicalOperatorNode) {
            val parent = node.input
            if (parent is EntityScanPhysicalOperatorNode) {
                val fetch = parent.fetch.toMap()
                val normalizedPredicate = this.normalize(node.predicate, fetch)

                /* Extract index hint and search for candidate. */
                val candidate = parent.entity.listIndexes().map {
                    parent.entity.indexForName(it).newTx(ctx)
                }.find {
                    it.state != IndexState.DIRTY && it.canProcess(normalizedPredicate)
                }

                if (candidate != null) {
                    val newFetch = parent.fetch.filter { candidate.columnsFor(normalizedPredicate).contains(it.second) }
                    val delta = parent.fetch.filter { !candidate.columnsFor(normalizedPredicate).contains(it.second) }
                    var p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, candidate, node.predicate, newFetch)
                    if (delta.isNotEmpty()) {
                        p = FetchPhysicalOperatorNode(p, parent.entity, delta)
                    }
                    return node.output?.copyWithOutput(p) ?: p
                }
            }
        }
        return null
    }

    /**
     * Normalizes the given [BooleanPredicate] given the list of mapped [ColumnDef]s. Normalization resolves
     * potential [ColumnDef] containing alias names to the root [ColumnDef]s.
     *
     * @param predicate [BooleanPredicate] To normalize.
     * @param fetch [Map] of [ColumnDef] and alias [Name.ColumnName].
     */
    private fun normalize(predicate: BooleanPredicate, fetch: Map<Binding.Column, ColumnDef<*>>): BooleanPredicate = when (predicate) {
        is BooleanPredicate.IsNull -> BooleanPredicate.IsNull(predicate.binding)
        is BooleanPredicate.Comparison -> BooleanPredicate.Comparison(
            when(val op = predicate.operator) {
                is ComparisonOperator.Equal -> ComparisonOperator.Equal(op.left, op.right)
                is ComparisonOperator.NotEqual -> ComparisonOperator.NotEqual(op.left, op.right)
                is ComparisonOperator.Greater -> ComparisonOperator.Greater(op.left, op.right)
                is ComparisonOperator.GreaterEqual -> ComparisonOperator.GreaterEqual(op.left, op.right)
                is ComparisonOperator.Less -> ComparisonOperator.Less(op.left, op.right)
                is ComparisonOperator.LessEqual -> ComparisonOperator.LessEqual(op.left,op.right)
                is ComparisonOperator.Like -> ComparisonOperator.Like(op.left, op.right)
                is ComparisonOperator.Match -> ComparisonOperator.Match(op.left, op.right)
                is ComparisonOperator.Between, /* IN and BETWEEN operators only support literal bindings. */
                is ComparisonOperator.In -> op
            })
        is BooleanPredicate.Not -> BooleanPredicate.Not(normalize(predicate.p, fetch))
        is BooleanPredicate.And -> BooleanPredicate.And(normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
        is BooleanPredicate.Or -> BooleanPredicate.Or(normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
        is BooleanPredicate.Literal -> predicate
    }
}