package org.vitrivr.cottontail.dbms.index.diskann.graph.primitives

import org.vitrivr.cottontail.dbms.index.diskann.graph.deg.AbstractDynamicExplorationGraph

/**
 * A [Distance] element produced by this [AbstractDynamicExplorationGraph].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Distance<I: Comparable<I>>(val label: I, val distance: Float): Comparable<Distance<I>> {
    override fun compareTo(other: Distance<I>): Int {
        val result = this.distance.compareTo(other.distance)
        return if (result == 0) {
            this.label.compareTo(other.label)
        } else {
            result
        }
    }

    /**
     * Returns this [Distance] element as a [Node] representation.
     *
     * @return [Node] representation of this [Distance]
     */
    fun asNode(): Node<I> = Node(this.label)
}