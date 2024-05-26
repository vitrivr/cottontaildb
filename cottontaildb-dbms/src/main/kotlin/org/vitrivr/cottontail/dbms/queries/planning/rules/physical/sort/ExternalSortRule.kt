package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.sort

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.ExternalSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.InMemorySortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize

/**
 * A [RewriteRule] that replaces a [InMemorySortPhysicalOperatorNode] by an [ExternalSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object ExternalSortRule: RewriteRule<OperatorNode.Physical> {
    /**
     * Applies this [ExternalSortRule] to the provided [OperatorNode], creating a new version of the tree.
     *
     * @param node The [OperatorNode.Physical] to apply this [LimitingSortPhysicalOperatorNode].
     * @param ctx The [QueryContext] used for planning.
     * @return Transformed [OperatorNode.Physical] or null, if transformation was not possible.
     */
    override fun tryApply(node: OperatorNode.Physical, ctx: QueryContext): OperatorNode.Physical? {
        if (node !is InMemorySortPhysicalOperatorNode) return null

        /* Perform rewrite. */
        val input = node.input.copyWithExistingInput()
        val tupleSize = input.statistics.estimateTupleSize()
        if (tupleSize > 0) {
            val chunkSize = Math.floorDiv(ctx.catalogue.config.memory.maxSortBufferSize, tupleSize).toInt()
            val p = ExternalSortPhysicalOperatorNode(input, node.sortOn, chunkSize)
            return node.output?.copyWithOutput(p) ?: p
        }
        return node
    }
}