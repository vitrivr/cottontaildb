package org.vitrivr.cottontail.core.queries.nodes

/**
 * A [Node] that can be copied.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
interface CopyableNode: Node {
    /**
     * Creates a copy of this [CopyableNode]. The copy must be built in such a ways, that all relevant data structures
     * that may be accessed concurrently, are copied.
     *
     * @return Copy of this [CopyableNode]
     */
    fun copy(): CopyableNode
}