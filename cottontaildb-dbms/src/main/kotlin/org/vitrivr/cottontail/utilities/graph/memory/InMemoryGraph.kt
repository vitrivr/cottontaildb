package org.vitrivr.cottontail.utilities.graph.memory

import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.utilities.graph.Graph

/**
 * An in memory implementation of the [Graph] interface.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class InMemoryGraph<V>(val maxDegree: Int = Int.MAX_VALUE): Graph<V> {

    /** An [Object2ObjectLinkedOpenHashMap] used as adjacency list for this [InMemoryGraph]. */
    private val map = Object2ObjectLinkedOpenHashMap<V, Object2FloatOpenHashMap<V>>()

    /**
     * Adds a new vertex of type [V] to this [InMemoryGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    override fun addVertex(v: V): Boolean {
        if (!this.map.containsKey(v)) {
            this.map[v] = Object2FloatOpenHashMap()
            return true
        }
        return false
    }

    /**
     * Removes a vertex of type [V] from this [InMemoryGraph].
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    override fun removeVertex(v: V): Boolean = (this.map.remove(v) != null)


    /**
     * Adds an edge between two vertices to this [Graph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @param weight The weight of the edge.
     * @return True on success, false otherwise.
     */
    override fun addEdge(from: V, to: V, weight: Float): Boolean {
        val e1 = this.map[from] ?: throw NoSuchElementException("The vertex $from does not exist in the graph." )
        val e2 = this.map[to] ?: throw NoSuchElementException("The vertex $to does not exist in the graph." )
        if (!e1.containsKey(to) && !e2.containsKey(from)) {
            check(e1.size < this.maxDegree) { "The vertex $from already has too many edges (maxDegree = ${this.maxDegree})." }
            check(e2.size < this.maxDegree) { "The vertex $from already has too many edges (maxDegree = ${this.maxDegree})." }
            e1[to] = weight
            e2[from] = weight
            return true
        }
        return false
    }

    /**
     * Removes an edge between two vertices to this [Graph]
     *
     * @param from The start vertex [V].
     * @param to The end vertex [V].
     * @return True on success, false otherwise.
     */
    override fun removeEdge(from: V, to: V): Boolean {
        val e1 = this.map[from] ?: throw NoSuchElementException("The vertex $from does not exist in the graph." )
        val e2 = this.map[to] ?: throw NoSuchElementException("The vertex $to does not exist in the graph." )
        if (e1.containsKey(to) && e2.containsKey(from)) {
            e1.removeFloat(to)
            e2.removeFloat(from)
            return true
        }
        return false
    }

    /**
     * Returns the number of vertexes in this [Graph].
     *
     * @return Number of vertexes in this [Graph].
     */
    override fun size(): Long = this.map.size.toLong()
    override fun edges(from: V): Map<V, Float> = this.map[from] ?: throw NoSuchElementException("The vertex $from does not exist in the graph.")

    /**
     *
     */
    override fun iterator(): Iterator<V> = this.map.keys.iterator()
}