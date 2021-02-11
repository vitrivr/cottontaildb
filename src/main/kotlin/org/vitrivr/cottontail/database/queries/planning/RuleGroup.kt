package org.vitrivr.cottontail.database.queries.planning

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RuleGroup] is a collection of [RewriteRule]s that can be applied to a [OperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class RuleGroup(val rules: Collection<RewriteRule>) {
    /**
     * Apples the rules contained in this [RuleGroup] to the given [OperatorNode]. If any rule
     * produces a result, it is added to the list of candidates
     *
     * @param expression The [OperatorNode] to apply rules to.
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return List of candidate [OperatorNode]s.
     */
    fun apply(expression: OperatorNode, ctx: QueryContext): List<OperatorNode> {
        val candidates = mutableListOf<OperatorNode>()
        for (rule in this.rules) {
            if (rule.canBeApplied(expression)) {
                val result = rule.apply(expression, ctx)
                if (result != null) {
                    candidates.add(result)
                }
            }
        }
        return candidates
    }
}