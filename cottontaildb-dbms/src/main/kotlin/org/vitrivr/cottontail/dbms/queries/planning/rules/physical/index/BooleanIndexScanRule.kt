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
        is BooleanPredicate.Atomic -> {
            /* Map left and right operands. */
            val op = predicate.operator
            val left = op.left
            val right: List<Binding> = when(op) {
                is ComparisonOperator.Binary -> listOf(op.right)
                is ComparisonOperator.Between -> {
                    listOf(op.rightLower, op.rightUpper)
                }
                else -> emptyList()
            }

            /* Return new operator. */
            val newOp = when(op) {
                is ComparisonOperator.Between -> ComparisonOperator.Between(left, right[0], right[1])
                is ComparisonOperator.Binary.Equal -> ComparisonOperator.Binary.Equal(left, right[0])
                is ComparisonOperator.Binary.Greater -> ComparisonOperator.Binary.Greater(left, right[0])
                is ComparisonOperator.Binary.GreaterEqual -> ComparisonOperator.Binary.GreaterEqual(left, right[0])
                is ComparisonOperator.Binary.Less -> ComparisonOperator.Binary.Less(left, right[0])
                is ComparisonOperator.Binary.LessEqual -> ComparisonOperator.Binary.LessEqual(left, right[0])
                is ComparisonOperator.Binary.Like -> ComparisonOperator.Binary.Like(left, right[0])
                is ComparisonOperator.Binary.Match -> ComparisonOperator.Binary.Match(left, right[0])
                is ComparisonOperator.In -> op /* IN operators only support literal bindings. */
                is ComparisonOperator.IsNull -> ComparisonOperator.IsNull(left)
            }
            BooleanPredicate.Atomic(newOp, predicate.not)
        }
        is BooleanPredicate.Compound.And -> BooleanPredicate.Compound.And(normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
        is BooleanPredicate.Compound.Or -> BooleanPredicate.Compound.Or(normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
    }
}