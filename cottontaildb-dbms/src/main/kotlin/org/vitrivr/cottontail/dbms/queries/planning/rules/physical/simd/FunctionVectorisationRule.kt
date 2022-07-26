package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd

import org.vitrivr.cottontail.core.queries.functions.VectorisableFunction
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that will be used to decide in which cases the usage of vectorized distance functions is beneficial.
 * The rule will be based on empiric data measurements.
 *
 * @author Colin Saladin & Ralph Gasser
 * @version 1.0.0
 */
class FunctionVectorisationRule(val threshold: Int) : RewriteRule {
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode &&
    node.function.function is VectorDistance<*>

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FunctionPhysicalOperatorNode && node.function.function is VectorisableFunction<*>) {
            val input = node.input?.copy() ?: return null
            val out = node.out
            val bindFunction = out.context.bind((node.function.function as VectorisableFunction<*>).vectorized(), node.function.arguments)

            // Provisional heuristic
            if ((node.function.function as VectorisableFunction<*>).vectorSize >= this.threshold) {
                val p = FunctionPhysicalOperatorNode(input, bindFunction, out)
                return node.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }
}