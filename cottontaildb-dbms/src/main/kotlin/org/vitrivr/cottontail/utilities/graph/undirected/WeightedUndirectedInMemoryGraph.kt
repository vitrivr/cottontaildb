package org.vitrivr.cottontail.utilities.graph.undirected

import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.basics.CloseableIterator
import org.vitrivr.cottontail.utilities.graph.Graph

/**
 * An undirected, in-memory implementation of the [Graph] interface.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class WeightedUndirectedInMemoryGraph<V>(private val maxDegree: Int = Int.MAX_VALUE): Graph<V> {

    /** An [Object2ObjectLinkedOpenHashMap] used as adjacency list for this [WeightedUndirectedInMemoryGraph]. */
    private val map = Object2ObjectLinkedOpenHashMap<V, Object2FloatOpenHashMap<V>>()

    /**
     * Returns the number of vertices [V] in this [Graph].
     *
     * @return Number of vertices [V] in this [Graph].
     */
    override fun size(): Long = this.map.size.toLong()

    /**
     * Returns an unmodifiable [Map] of all edges from the given Vertex [V] in this [WeightedUndirectedInMemoryGraph].
     *
     * @return [Map] of all edges from [V] in this [WeightedUndirectedInMemoryGraph].
     */
    override fun edges(from: V): Map<V,Float> = this.map[from] ?: throw NoSuchElementException("The vertex $from does not exist in the graph.")

    /**
     * Adds a new vertex of type [V] to this [WeightedUndirectedInMemoryGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    override fun addVertex(v: V): Boolean {
        if (!this.map.containsKey(v)) {
            val edges = Object2FloatOpenHashMap<V>()
            edges.defaultReturnValue(Float.MIN_VALUE)
            this.map[v] = edges
            return true
        }
        return false
    }

    /**
     * Removes a vertex of type [V] from this [WeightedUndirectedInMemoryGraph].
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    override fun removeVertex(v: V): Boolean {
        val edges = this.map.remove(v) ?: return false
        edges.keys.forEach { this.map[it]?.removeFloat(v) }
        return true
    }

    /**
     * Adds an edge between two vertices to this [Graph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @param weight The weight of the edge.
     * @return True on success, false otherwise.
     */
    override fun addEdge(from: V, to: V, weight: Float): Boolean {
        /* Sanity checks. */
        require(from != to) { "Failed to add edge: FROM and TO vertex are the same."}
        val e1 = this.map[from] ?: throw NoSuchElementException("Failed to add edge: FROM vertex $from does not exist in the graph." )
        val e2 = this.map[to] ?: throw NoSuchElementException("Failed to add edge: TO vertex $to does not exist in the graph." )

        /* Add edge if it does not exist. */
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
        /* Sanity checks. */
        require(from != to) { "Failed to add edge: FROM and TO vertex are the same."}
        val e1 = this.map[from] ?: throw NoSuchElementException("Failed to add edge: FROM vertex $from does not exist in the graph." )
        val e2 = this.map[to] ?: throw NoSuchElementException("Failed to add edge: TO vertex $to does not exist in the graph." )

        /* Remove edge if it exists. */
        if (e1.containsKey(to) && e2.containsKey(from)) {
            e1.removeFloat(to)
            e2.removeFloat(from)
            return true
        }
        return false
    }

    /**
     *
     */
    override fun weight(from: V, to: V): Float {
        val e = this.map[from] ?: return Float.MIN_VALUE
        return e.getFloat(to)
    }


    /**
     *
     */
    override fun vertices(): CloseableIterator<V> = object : CloseableIterator<V> {
        private val iterator = this@WeightedUndirectedInMemoryGraph.map.keys.iterator()
        override fun hasNext(): Boolean = this.iterator.hasNext()
        override fun next(): V = this.iterator.next()
        override fun close() {/* No op */ }
    }
}