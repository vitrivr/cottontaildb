package org.vitrivr.cottontail.core.queries.nodes

/**
 * A [Node] that has an internal state and must therefore be copied to avoid concurrency issues.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface StatefulNode {
    /**
     * Creates a copy of this [StatefulNode]
     *
     * @return Copy of this [StatefulNode].
     */
    fun copy(): StatefulNode
}