package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] preceded by a
 * [EntityScanLogicalOperatorNode] through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean =
        node is FilterPhysicalOperatorNode && node.input is EntityScanPhysicalOperatorNode

    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
        if (node is FilterPhysicalOperatorNode) {
            val parent = node.input
            if (parent is EntityScanPhysicalOperatorNode) {
                val fetch = parent.fetch.toMap()
                val normalizedPredicate = this.normalize(node.predicate, fetch)
                val indexes = parent.entity.listIndexes()
                val candidate = indexes.map {
                    parent.entity.indexForName(it)
                }.find {
                    it.state != IndexState.DIRTY && it.canProcess(normalizedPredicate)
                }
                if (candidate != null) {
                    val newFetch = parent.fetch.filter { candidate.produces.contains(it.second) }
                    val delta = parent.fetch.filter { !candidate.produces.contains(it.second) }
                    var p: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, node.predicate, newFetch)
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
            val left = if (op.left is Binding.Column) {
                Binding.Column(fetch[(op.left as Binding.Column)]!!, op.left.context)
            } else {
                op.left
            }
            val right: List<Binding> = when(op) {
                is ComparisonOperator.Binary -> {
                    listOf(if (op.right is Binding.Column) {
                        Binding.Column(fetch[(op.right as Binding.Column)]!!, op.right.context)
                    } else {
                        op.right
                    })
                }
                is ComparisonOperator.Between -> {
                    listOf(
                        if (op.rightLower is Binding.Column) {
                            Binding.Column(fetch[(op.rightLower as Binding.Column)]!!, op.rightLower.context)
                        } else {
                            op.rightLower
                        },
                        if (op.rightUpper is Binding.Column) {
                            Binding.Column(fetch[(op.rightUpper as Binding.Column)]!!, op.rightUpper.context)
                        } else {
                            op.rightUpper
                        }
                    )
                }
                else -> emptyList<Binding>()
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
            BooleanPredicate.Atomic(newOp, predicate.not, predicate.dependsOn)
        }
        is BooleanPredicate.Compound.And -> BooleanPredicate.Compound.And(normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
        is BooleanPredicate.Compound.Or -> BooleanPredicate.Compound.Or(normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
    }
}