package org.vitrivr.cottontail.dbms.queries.planning.rules

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode

/**
 * A [RewriteRule] is a rule used during query planning. It tries to replace an [OperatorNode]
 * or a combination of [OperatorNode]s through another, more optimal combination.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface RewriteRule<T: OperatorNode> {
    /**
     * Transforms the given [OperatorNode] (and potentially its parents and children) to a new
     * [OperatorNode] that produces the equivalent output.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] used for planning.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    fun tryApply(node: T, ctx: QueryContext): T?
}