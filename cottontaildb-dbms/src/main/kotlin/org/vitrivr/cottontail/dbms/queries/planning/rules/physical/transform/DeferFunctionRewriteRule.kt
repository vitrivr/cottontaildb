package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers execution of a [FunctionPhysicalOperatorNode]. This can be beneficial in presence of, e.g., s,k-selections.
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
object DeferFunctionRewriteRule: RewriteRule {
    /**
     * The [DeferFunctionRewriteRule] can be applied to all [FunctionPhysicalOperatorNode]s.
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [DeferFunctionRewriteRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode

    /**
     * Apples this [DeferFunctionRewriteRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        /* Make sure, that node is a FetchLogicalOperatorNode. */
        require(node is FunctionPhysicalOperatorNode) { "Called DeferFetchOnFetchRewriteRule.rewrite() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}

        /* Check for early abort; if next node requires all candidates. */
        val originalGroupId = node.groupId
        var next: OperatorNode.Physical? = node.output
        var copy: OperatorNode.Physical = node.input.copyWithExistingInput()

        /* Check if we encounter a node that requires this column; if so defer function invocation. */
        while (next != null && next.groupId == originalGroupId) {
            if (next.requires.contains(node.out)) {
                return if (next == node.output) {
                    null
                } else {
                    next.copyWithOutput(node.copyWithNewInput(copy))
                }
            }

            /* Move to next nodes. */
            copy = append(copy, next)
            next = next.output
        }
        return null
    }

    /**
     * This is an internal method: It can be used to build up an [OperatorNode.Logical] tree [OperatorNode.Logical] by [OperatorNode.Logical],
     * by appending the [next] [OperatorNode.Logical] to the [current] [OperatorNode.Logical] and returning the [next] [OperatorNode.Logical].
     *
     * @param current [OperatorNode.Logical]
     * @param next [OperatorNode.Logical]
     * @return [OperatorNode] that is the new current.
     */
    private fun append(current: OperatorNode.Physical, next: OperatorNode.Physical): OperatorNode.Physical = when (next) {
        is UnaryPhysicalOperatorNode ->  next.copyWithNewInput(current)
        is BinaryPhysicalOperatorNode -> next.copyWithNewInput(current, next.right.copyWithExistingInput())
        is NAryPhysicalOperatorNode -> next.copyWithNewInput(current, *next.inputs.drop(1).map { it.copyWithExistingInput() }.toTypedArray())
        else -> throw IllegalArgumentException("Encountered an unsupported node during execution of DeferredFetchRewriteRule.")
    }
}