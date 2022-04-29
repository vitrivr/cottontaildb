package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Decomposes a [FilterOnSubSelectLogicalOperatorNode] that contains a [BooleanPredicate.Compound.And] into a sequence
 * of two [FilterOnSubSelectLogicalOperatorNode]s or a [FilterOnSubSelectLogicalOperatorNode] and a [FilterLogicalOperatorNode].
 *
 * Gives precedence to the left operand.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object LeftConjunctionOnSubselectRewriteRule : RewriteRule {

    /**
     * The [LeftConjunctionOnSubselectRewriteRule] can be applied to all [FilterOnSubSelectLogicalOperatorNode]s that contain a [BooleanPredicate.Compound.And].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [FilterOnSubSelectLogicalOperatorNode] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean =
        node is FilterOnSubSelectLogicalOperatorNode && node.predicate is BooleanPredicate.Compound.And

    /**
     * Decomposes the provided [FilterOnSubSelectLogicalOperatorNode] with a conjunction into two consecutive
     * [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode] covers one part of the conjunction.
     * Gives precedence to the left part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {
        /* Make sure, that node is a FilterOnSubSelectLogicalOperatorNode. */
        require(node is FilterOnSubSelectLogicalOperatorNode) { "Called LeftConjunctionOnSubselectRewriteRule.apply() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}
        require(node.predicate is BooleanPredicate.Compound.And) { "Called LeftConjunctionOnSubselectRewriteRule.apply() with node a predicate that is not a conjunction. This is a programmer's error!" }

        val parent = node.inputs[0].copyWithInputs()
        val p1HasSubselect = node.predicate.p1.atomics.any { a ->
            val op = a.operator
            op is ComparisonOperator.In && op.right.any { !it.static }
        }
        val p2HasSubselect = node.predicate.p2.atomics.any { a ->
            val op = a.operator
            op is ComparisonOperator.In && op.right.any { !it.static }
        }

        val p1Filter = if (p1HasSubselect) {
            FilterOnSubSelectLogicalOperatorNode(node.predicate.p1, parent)
        } else {
            FilterLogicalOperatorNode(parent, node.predicate.p1)
        }

        val p2Filter = if (p2HasSubselect) {
            FilterOnSubSelectLogicalOperatorNode(node.predicate.p2, p1Filter)
        } else {
            FilterLogicalOperatorNode(p1Filter, node.predicate.p2)
        }

        return node.output?.copyWithOutput(p2Filter) ?: p2Filter
    }
}