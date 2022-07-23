package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 *
 * @author Ralph Gasser
 * @version 1.0
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
        var copy: OperatorNode.Physical = node.input!!.copyWithInputs()

        /* Check if we encounter a node that requires this column; if so defer function invocation. */
        while (next != null && next.groupId == originalGroupId) {
            if (next.requires.contains(node.out.column)) {
                return if (next == node.output) {
                    null
                } else {
                    next.copyWithOutput(node.copy(copy))
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
        is UnaryPhysicalOperatorNode -> {
            val p = next.copy()
            p.input = current
            p
        }
        is BinaryPhysicalOperatorNode -> {
            val p = next.copy()
            p.left = current
            p.right = next.right?.copyWithInputs()
            p
        }
        is NAryPhysicalOperatorNode -> {
            val p = next.copy()
            p.addInput(current)
            for (it in next.inputs.drop(1)) {
                p.addInput(it.copyWithInputs())
            }
            p
        }
        else -> throw IllegalArgumentException("Encountered unsupported node during execution of DeferredFetchRewriteRule.")
    }
}