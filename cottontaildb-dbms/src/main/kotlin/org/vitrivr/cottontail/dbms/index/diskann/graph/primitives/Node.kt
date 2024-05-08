package org.vitrivr.cottontail.dbms.index.diskann.graph.primitives

/**
 * A [Node] in an [AbstractDynamicExplorationGraph].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@JvmInline
value class Node<I: Comparable<I>>(val label: I): Comparable<Node<I>> {
    override fun compareTo(other: Node<I>): Int = this.label.compareTo(other.label)
}