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
    val size: Long

    /**
     * Returns all edges of a vertex [V] in this [MutableGraph].
     *
     * @param from The vertex [V] for which to return the edges.
     * @return [Map] of edges and their weights.
     */
    fun edges(from: V): Map<V,Float>

    /**
     * Creates and returns a [CloseableIterator] for this [MutableGraph].
     *
     * @return A [CloseableIterator] over all vertices [V] in this [MutableGraph].
     */
    fun vertices(): CloseableIterator<V>

    /**
     * Returns the weight from one vertex [V] to another vertex [V] in this [MutableGraph].
     *
     * @param from The vertex [V] from which the edge should lead.
     * @param from The vertex [V] to which the edge should lead.
     * @return [Float] weight of the edge.
     */
    fun weight(from: V, to: V): Float
}