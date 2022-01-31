package org.vitrivr.cottontail.dbms.queries.planning.rules.logical

import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns fetched in a [FetchLogicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object DeferFetchOnFetchRewriteRule : RewriteRule {
    override fun canBeApplied(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode): Boolean = node is FetchLogicalOperatorNode
    override fun apply(node: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, ctx: QueryContext): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode? {
        if (node is FetchLogicalOperatorNode) {
            /* Copy tree up and until the fetch operation; append reduced FetchLogicalOperatorNode if not fetching of all nodes can be deferred. */
            val candidates = node.fetch.map { it.first to it.second }.toMutableList()
            val originalGroupId = node.groupId
            var copy: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical = node.input!!.copyWithInputs()

            /* Check for early abort; if next node requires all candidates. */
            if (candidates.all { node.output?.requires?.contains(it.first.column) == true }) {
                return null
            }

            /* Traverse tree and push down FetchLogicalOperatorNode. */
            var next: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical? = node.output
            while (next != null && next.groupId == originalGroupId) {
                /* Append FetchLogicalOperatorNode for columns required by next element. */
                val required = candidates.filter { it.first.column in next!!.requires }
                if (required.isEmpty()) {
                    /* Case 1: Next node has no requirement. Simply append node to tree. */
                    copy = append(copy, next)
                } else {
                    /* Case 2: Next node has a requirement. Fetch required (column)s and continue build-up. */
                    copy = FetchLogicalOperatorNode(copy, node.entity, required.map { it.first to it.second })
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
        } else {
            return null
        }
    }

    /**
     * This is an internal method: It can be used to build up an [OperatorNode.Logical] tree [OperatorNode.Logical] by [OperatorNode.Logical],
     * by appending the [next] [OperatorNode.Logical] to the [current] [OperatorNode.Logical] and returning the [next] [OperatorNode.Logical].
     *
     * @param current [OperatorNode.Logical]
     * @param next [OperatorNode.Logical]
     * @return [OperatorNode] that is the new current.
     */
    private fun append(current: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical, next: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical = when (next) {
        is UnaryLogicalOperatorNode -> {
            val p = next.copy()
            p.input = current
            p
        }
        is org.vitrivr.cottontail.dbms.queries.operators.logical.BinaryLogicalOperatorNode -> {
            val p = next.copy()
            p.left = current
            p.right = next.right?.copyWithInputs()
            p
        }
        is NAryLogicalOperatorNode -> {
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