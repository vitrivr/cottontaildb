package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.sort

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.ExternalSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.InMemorySortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize

/**
 * A [RewriteRule] that replaces a [In] followed by a [LimitPhysicalOperatorNode]
 * into a [LimitingSortPhysicalOperatorNode]
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
object ExternalSortRule: RewriteRule {
    /**
     * The [ExternalSortRule] can be applied to all [InMemorySortPhysicalOperatorNode]s.
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [InMemorySortPhysicalOperatorNode] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is InMemorySortPhysicalOperatorNode

    /**
     * Apples this [LimitingSortPhysicalOperatorNode] to the provided [OperatorNode], creating a new version of the tree.
     *
     * @param node The [OperatorNode] to apply this [LimitingSortPhysicalOperatorNode].
     * @param ctx The [QueryContext] used for planning.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode {
        /* Make sure, that node is a LimitPhysicalOperatorNode. */
        require(node is InMemorySortPhysicalOperatorNode) { "Called LimitingSortMergeRule.apply() with node of type ${node.javaClass.simpleName} that is not a LimitPhysicalOperatorNode. This is a programmer's error!"}

        /* Perform rewrite. */
        val input = node.input.copyWithExistingInput()
        val tupleSize = input.statistics.estimateTupleSize()
        val chunkSize = Math.floorDiv(ctx.catalogue.config.memory.maxSortBufferSize, tupleSize).toInt()
        val p = ExternalSortPhysicalOperatorNode(input, node.sortOn, chunkSize)
        return node.output?.copyWithOutput(p) ?: p
    }
}