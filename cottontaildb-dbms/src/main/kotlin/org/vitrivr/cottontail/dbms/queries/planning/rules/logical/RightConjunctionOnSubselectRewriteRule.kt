package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.PlaceholderLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * Decomposes a [FilterOnSubSelectLogicalOperatorNode] that contains a [BooleanPredicate.Compound.And] into a sequence
 * of two [FilterOnSubSelectLogicalOperatorNode]s or a [FilterOnSubSelectLogicalOperatorNode] and a [FilterLogicalOperatorNode].
 *
 * Gives precedence to the right operand.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object RightConjunctionOnSubselectRewriteRule : RewriteRule {

    /**
     * The [RightConjunctionOnSubselectRewriteRule] can be applied to all [FilterOnSubSelectLogicalOperatorNode]s that contain a [BooleanPredicate.Compound.And].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [FilterOnSubSelectLogicalOperatorNode] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean =
        node is FilterOnSubSelectLogicalOperatorNode && node.predicate is BooleanPredicate.Compound.And

    /**
     * Decomposes the provided [FilterOnSubSelectLogicalOperatorNode] with a conjunction into two consecutive
     * [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode] covers
     * one part of the conjunction. Gives precedence to the left part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {

        /* Make sure, that node is a FilterOnSubSelectLogicalOperatorNode. */
        require(node is FilterOnSubSelectLogicalOperatorNode) { "Called RightConjunctionOnSubselectRewriteRule.apply() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}
        require(node.predicate is BooleanPredicate.Compound.And) { "Called RightConjunctionOnSubselectRewriteRule.apply() with node a predicate that is not a conjunction. This is a programmer's error!" }

        val parent = node.right.copyWithExistingInput()
        val p1HasSubselect = node.predicate.p1.atomics.any { a ->
            val op = a.operator
            op is ComparisonOperator.In && op.right.any { !it.static }
        }
        val p2HasSubselect = node.predicate.p2.atomics.any { a ->
            val op = a.operator
            op is ComparisonOperator.In && op.right.any { !it.static }
        }

        val p2Filter = if (p2HasSubselect) {
            FilterOnSubSelectLogicalOperatorNode(node.predicate.p2, parent, PlaceholderLogicalOperatorNode(node.right.groupId, node.right.columns, node.right.physicalColumns))
        } else {
            FilterLogicalOperatorNode(parent, node.predicate.p2)
        }

        val p1Filter = if (p1HasSubselect) {
            FilterOnSubSelectLogicalOperatorNode(node.predicate.p1, p2Filter, PlaceholderLogicalOperatorNode(node.right.groupId, node.right.columns, node.right.physicalColumns))
        } else {
            FilterLogicalOperatorNode(p2Filter, node.predicate.p1)
        }

        return node.output?.copyWithOutput(p1Filter) ?: p1Filter
    }
}