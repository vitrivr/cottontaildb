package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd

import org.vitrivr.cottontail.core.queries.functions.VectorisableFunction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that will be used to decide in which cases the usage of vectorized distance functions is beneficial.
 * The rule will be based on empiric data measurements.
 *
 * @author Colin Saladin
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FunctionVectorisationRule(val threshold: Int) : RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [FunctionVectorisationRule] and tries to replace a [EntityScanPhysicalOperatorNode] followed by a [FilterLogicalOperatorNode]
     *
     * @param node The [OperatorNode.Physical] that should be processed.
     * @param ctx The [QueryContext] in which this rule is applied.
     * @return Transformed [OperatorNode.Physical] or null, if transformation was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        /* Extract necessary components. */
        if (node !is FunctionPhysicalOperatorNode) return null
        val function = node.function.function as? VectorisableFunction<*> ?: return null

        /* Provisional heuristic. */
        if (function.vectorSize >= this.threshold) {
            val input = node.input.copyWithExistingInput()
            val out = node.out
            val bindFunction = ctx.bindings.bind(function.vectorized(), node.function.arguments)
            val p = FunctionPhysicalOperatorNode(input, bindFunction, out)
            return node.output?.copyWithOutput(p) ?: p
        }
        return null
    }
}