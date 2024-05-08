package org.vitrivr.cottontail.utilities.graph

/**
 * A simple, weighted [Graph] data structure on elements of type [V].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Graph<V>: Iterable<V> {
    /**
     * Returns the number of vertexes in this [Graph].
     *
     * @return Number of vertexes in this [Graph].
     */
    fun size(): Long

    /**
     * Adds a new vertex of type [V] to this [Graph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    fun addVertex(v: V): Boolean

    /**
     * Removes a vertex of type [V] from this [Graph].
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    fun removeVertex(v: V): Boolean

    /**
     * Adds an edge between two vertices to this [Graph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @return True on success, false otherwise.
     */
    fun addEdge(from: V, to: V): Boolean = addEdge(from, to, 0.0f)

    /**
     * Adds an edge between two vertices to this [Graph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @param weight The weight of the edge.
     * @return True on success, false otherwise.
     */
    fun addEdge(from: V, to: V, weight: Float): Boolean

    /**
     * Removes an edge between two vertices to this [Graph]
     *
     * @param from The start vertex [V].
     * @param to The end vertex [V].
     * @return True on success, false otherwise.
     */
    fun removeEdge(from: V, to: V): Boolean

    /**
     * Returns all edges of a vertex [V] in this [Graph].
     *
     * @param from The vertex [V] for which to return the edges.
     * @return [Map] of edges and their weights.
     */
    fun edges(from: V): Map<V,Float>

    /**
     * Returns the weight from one vertex [V] to another vertex [V] in this [Graph].
     *
     * @param from The vertex [V] from which the edge should lead.
     * @param from The vertex [V] to which the edge should lead.
     * @return [Float] weight of the edge.
     */
    fun weight(from: V, to: V): Float
}