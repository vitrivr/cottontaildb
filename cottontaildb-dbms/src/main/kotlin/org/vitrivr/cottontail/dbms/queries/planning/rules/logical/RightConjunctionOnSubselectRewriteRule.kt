package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate

/**
 * Decomposes a [FilterOnSubSelectLogicalOperatorNode] that contains a [BooleanPredicate.Compound.And] into a sequence
 * of two [FilterOnSubSelectLogicalOperatorNode]s or a [FilterOnSubSelectLogicalOperatorNode] and a [FilterLogicalOperatorNode].
 *
 * Gives precedence to the right operand.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object RightConjunctionOnSubselectRewriteRule : RewriteRule {

    /**
     * Checks if this [LeftConjunctionOnSubselectRewriteRule] can be applied to the given [OperatorNode].
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean =
        node is FilterOnSubSelectLogicalOperatorNode && node.predicate is BooleanPredicate.Compound.And


    /**
     * Decomposes the provided [FilterOnSubSelectLogicalOperatorNode] with a conjunction into two consecutive
     * [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode] covers
     * one part of the conjunction. Gives precedence to the left part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
        if (node is FilterOnSubSelectLogicalOperatorNode && node.predicate is BooleanPredicate.Compound.And) {
            val parent = node.inputs[0].copyWithInputs()
            val p1DependsOn = node.predicate.p1.atomics.filter { it.dependsOn > -1 }
            val p2DependsOn = node.predicate.p2.atomics.filter { it.dependsOn > -1 }

            val p2Filter = if (p2DependsOn.isNotEmpty()) {
                FilterOnSubSelectLogicalOperatorNode(node.predicate.p2, parent)
            } else {
                FilterLogicalOperatorNode(parent, node.predicate.p2)
            }

            val p1Filter = if (p1DependsOn.isNotEmpty()) {
                FilterOnSubSelectLogicalOperatorNode(node.predicate.p1, p2Filter)
            } else {
                FilterLogicalOperatorNode(p2Filter, node.predicate.p1)
            }

            return node.output?.copyWithOutput(p1Filter) ?: p1Filter
        }
        return null
    }
}