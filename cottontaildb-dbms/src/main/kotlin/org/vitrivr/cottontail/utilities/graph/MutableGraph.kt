package org.vitrivr.cottontail.utilities.graph

/**
 * A mutable [Graph] on elements of type [V].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface MutableGraph<V>: Graph<V> {
    /**
     * Adds a new vertex of type [V] to this [MutableGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    fun addVertex(v: V): Boolean

    /**
     * Removes a vertex of type [V] from this [MutableGraph].
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    fun removeVertex(v: V): Boolean

    /**
     * Adds an edge between two vertices to this [MutableGraph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @return True on success, false otherwise.
     */
    fun addEdge(from: V, to: V): Boolean = addEdge(from, to, 0.0f)

    /**
     * Adds an edge between two vertices to this [MutableGraph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @param weight The weight of the edge.
     * @return True on success, false otherwise.
     */
    fun addEdge(from: V, to: V, weight: Float): Boolean

    /**
     * Removes an edge between two vertices to this [MutableGraph]
     *
     * @param from The start vertex [V].
     * @param to The end vertex [V].
     * @return True on success, false otherwise.
     */
    fun removeEdge(from: V, to: V): Boolean
}