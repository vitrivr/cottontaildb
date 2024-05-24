package org.vitrivr.cottontail.utilities.graph

import org.vitrivr.cottontail.core.basics.CloseableIterator

/**
 * A simple, weighted [Graph] data structure on elements of type [V].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Graph<V> {
    /** The number of vertices in this [MutableGraph]. */
    val size: Int

    /**
     * Checks if this [Graph] contains an edge from vertex [V] to vertex [V].
     *
     * @param from The from vertex [V].
     * @param to THe to vertex[V]
     * @return True if an edge exists, false otherwise.
     */
    fun hasEdge(from: V, to: V): Boolean

    /**
     * Returns all edges of a vertex [V] in this [MutableGraph].
     *
     * @param from The vertex [V] for which to return the edges.
     * @return [Edge] of edges and their weights.
     */
    fun edges(from: V): List<Edge<V>>

    /**
     * Creates and returns a [CloseableIterator] for this [MutableGraph].
     *
     * @return A [CloseableIterator] over all vertices [V] in this [MutableGraph].
     */
    fun vertices(): Iterator<V>

    /**
     * Returns the weight from one vertex [V] to another vertex [V] in this [MutableGraph].
     *
     * @param from The vertex [V] from which the edge should lead.
     * @param from The vertex [V] to which the edge should lead.
     * @return [Float] weight of the edge.
     */
    fun weight(from: V, to: V): Float
}