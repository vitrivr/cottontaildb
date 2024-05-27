package org.vitrivr.cottontail.utilities.graph.undirected

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.utilities.graph.Edge
import org.vitrivr.cottontail.utilities.graph.MutableGraph

/**
 * An undirected, in-memory implementation of the [MutableGraph] interface.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class WeightedUndirectedInMemoryGraph<V>(private val maxDegree: Int, private val adjacencyList: MutableMap<V, MutableList<Edge<V>>>): MutableGraph<V> {

    /**
     * Simple constructor for an empty [WeightedUndirectedInMemoryGraph].
     */
    constructor(maxDegree: Int): this(maxDegree, Object2ObjectOpenHashMap<V,MutableList<Edge<V>>>())

    /** The number of vertices in this [WeightedUndirectedInMemoryGraph]. */
    override val size: Int
        get() = this.adjacencyList.size

    /**
     * Returns an unmodifiable [Map] of all edges from the given Vertex [V] in this [WeightedUndirectedInMemoryGraph].
     *
     * @return [Map] of all edges from [V] in this [WeightedUndirectedInMemoryGraph].
     */
    override fun edges(from: V): List<Edge<V>> = this.adjacencyList[from] ?: throw NoSuchElementException("The vertex $from does not exist in the graph.")

    /**
     * Adds a new vertex of type [V] to this [WeightedUndirectedInMemoryGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    override fun addVertex(v: V) {
        this.adjacencyList.computeIfAbsent(v) { _: V -> ArrayList(this.maxDegree) }
    }

    /**
     * Removes a vertex of type [V] from this [WeightedUndirectedInMemoryGraph].
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    override fun removeVertex(v: V) {
        val edges = this.adjacencyList.remove(v)
        if (edges != null) {
            for (edge in edges) {
                this.adjacencyList[edge.to]?.removeIf { it.to == v }
            }
        }
    }

    /**
     * Adds an edge between two vertices to this [MutableGraph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @param weight The weight of the edge.
     * @return True on success, false otherwise.
     */
    override fun addEdge(from: V, to: V, weight: Float): Boolean {
        require(from != to) { "Failed to add edge: FROM and TO vertex are the same."}

        /* Obtain list entries. */
        val e1 = this.adjacencyList[from] ?: throw NoSuchElementException("Failed to add edge: FROM vertex $from does not exist in the graph." )
        val e2 = this.adjacencyList[to] ?: throw NoSuchElementException("Failed to add edge: TO vertex $to does not exist in the graph." )

        /* Sanity checks. */
        check(e1.size < this.maxDegree) { "Failed to add edge: FROM vertex $to has reached maximum degree." }
        check(e2.size < this.maxDegree) { "Failed to add edge: TO vertex $from has reached maximum degree." }

        /* Add edge if it does not exist. */
        if (e1.any { it.to == to } || e2.any { it.to == from }) {
            return false
        } else {
            e1.add(Edge(to, weight))
            e2.add(Edge(from, weight))
            return false
        }
    }

    /**
     * Removes an edge between two vertices to this [MutableGraph]
     *
     * @param from The start vertex [V].
     * @param to The end vertex [V].
     * @return True on success, false otherwise.
     */
    override fun removeEdge(from: V, to: V): Boolean {
        /* Sanity checks. */
        require(from != to) { "Failed to add edge: FROM and TO vertex are the same."}
        val e1 = this.adjacencyList[from] ?: throw NoSuchElementException("Failed to add edge: FROM vertex $from does not exist in the graph." )
        val e2 = this.adjacencyList[to] ?: throw NoSuchElementException("Failed to add edge: TO vertex $to does not exist in the graph." )

        /* Remove edge if it exists. */
        return e1.removeIf { it.to == to } && e2.removeIf { it.to == from }
    }

    /**
     * Checks if this [WeightedUndirectedInMemoryGraph] contains an edge from vertex [V] to vertex [V].
     *
     * @param from The vertex [V] from which the edge should lead.
     * @param to The [V] to which the edge should lead.
     * @return True if an edge exists, false otherwise.
     */
    override fun hasEdge(from: V, to: V): Boolean {
        val edges = this.adjacencyList[from] ?: return false
        return edges.any { it.to == to }
    }

    /**
     * Returns the weight from one vertex [V] to another vertex [V] in this [WeightedUndirectedInMemoryGraph].
     *
     * @param from The vertex [V] from which the edge should lead.
     * @param to The [V] to which the edge should lead.
     * @return [Float] weight of the edge or [Float.MIN_VALUE], if no such [Edge] exists.
     */
    override fun weight(from: V, to: V): Float {
        val edges = this.adjacencyList[from] ?: return Float.MIN_VALUE
        return edges.find { it.to == to }?.weight ?: return Float.MIN_VALUE
    }

    /**
     * Creates an [Iterator] over the vertices [V] in this [WeightedUndirectedInMemoryGraph].
     *
     * @return [Iterator] of [V]
     */
    override fun vertices(): Iterator<V> = this@WeightedUndirectedInMemoryGraph.adjacencyList.keys.iterator()

    /**
     * Clears this [WeightedUndirectedInMemoryGraph].
     */
    override fun clear() {
        this.adjacencyList.clear()
    }
}