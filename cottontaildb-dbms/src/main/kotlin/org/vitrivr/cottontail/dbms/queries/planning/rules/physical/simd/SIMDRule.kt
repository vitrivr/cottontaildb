package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd

import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that will be used to decide in which cases the usage of vectorized distance functions is beneficial.
 * The rule will be based on empiric data measurements.
 *
 * @author Colin Saladin
 * @version 1.3.0
 */
object SIMDRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean {
        if (node is FunctionPhysicalOperatorNode && node.function.function is VectorDistance<*>) {
            return true
        }
        return false
    }

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        // TODO @Colin("Not yet implemented")
        if (node is FunctionPhysicalOperatorNode && node.function.function is VectorDistance<*>) {
            var bindingFunction = node.out.context.bind((node.function.function as VectorDistance<*>).vectorized(), node.function.arguments)
            if ((node.function.function as VectorDistance<*>).type.logicalSize >= 250) {
                val p = FunctionPhysicalOperatorNode(node.input?.copyWithOutput(), bindingFunction, node.out.copy())
                return p
            }
        }
        return null
    }

}