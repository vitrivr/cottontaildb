package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform

import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.NNSIndexScanClass1Rule

/**
 * A [RewriteRule] that defers fetching of columns fetched in a [FetchPhysicalOperatorNode].
 *
 * This is sometimes necessary, because certain replacements may generate unnecessary fetches (e.g. [NNSIndexScanClass1Rule])
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
object DeferFetchOnFetchRewriteRule: RewriteRule {
    /**
     * The [DeferFetchOnFetchRewriteRule] can be applied to all [FetchPhysicalOperatorNode]s.
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return True if [DeferFetchOnFetchRewriteRule] can be applied to [node], false otherwise.
     */
    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FetchPhysicalOperatorNode

    /**
     * Apples this [DeferFetchOnFetchRewriteRule] to the provided [OperatorNode].
     *
     * @param node The [OperatorNode] to check.
     * @param ctx The [QueryContext]
     * @return [OperatorNode] or null, if rewrite was not possible.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        /* Make sure, that node is a FetchLogicalOperatorNode. */
        require(node is FetchPhysicalOperatorNode) { "Called DeferFetchOnFetchRewriteRule.rewrite() with node of type ${node.javaClass.simpleName}. This is a programmer's error!"}

        /* Copy tree up and until the fetch operation; append reduced FetchLogicalOperatorNode if not fetching of all nodes can be deferred. */
        val candidates = node.fetch.toMutableList()
        val originalGroupId = node.groupId
        var copy: OperatorNode.Physical = node.input.copyWithExistingInput()

        /* Check for early abort; if next node requires all candidates. */
        if (candidates.all { node.output?.requires?.contains(it) == true }) {
            return null
        }

        /* Traverse tree and push down FetchLogicalOperatorNode. */
        var next: OperatorNode.Physical? = node.output
        while (next != null && next.groupId == originalGroupId) {
            /* Append FetchLogicalOperatorNode for columns required by next element. */
            val required = candidates.filter { it in next!!.requires }
            if (required.isEmpty()) {
                /* Case 1: Next node has no requirement. Simply append node to tree. */
                copy = append(copy, next)
            } else {
                /* Case 2: Next node has a requirement. Fetch required (column)s and continue build-up. */
                copy = FetchPhysicalOperatorNode(copy, node.tx, required)
                candidates.removeIf { required.contains(it) }
                if (candidates.isEmpty()) {
                    copy = next.copyWithOutput(copy)
                    break
                } else {
                    copy = append(copy, next)
                }
            }

            /* Continue until end of tree has been reached. */
            next = next.output
        }
        return copy
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