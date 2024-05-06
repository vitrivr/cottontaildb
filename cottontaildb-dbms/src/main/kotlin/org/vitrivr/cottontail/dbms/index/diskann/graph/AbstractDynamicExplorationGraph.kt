package org.vitrivr.cottontail.dbms.index.diskann.graph

import it.unimi.dsi.fastutil.objects.Object2FloatLinkedOpenHashMap
import org.apache.lucene.search.Weight
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.utilities.graph.Graph
import java.lang.Math.floorDiv
import java.util.*
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.math.max

/**
 * This class implements a Dynamic Exploration Graph (DEG) as proposed in [1]. It can be used to perform approximate nearest neighbour search (ANNS).
 *
 * Literature:
 * [1] Hezel, Nico, et al. "Fast Approximate Nearest Neighbor Search with a Dynamic Exploration Graph using Continuous Refinement." arXiv preprint arXiv:2307.10479 (2023)
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractDynamicExplorationGraph<I,V>(private val degree: Int, val graph: Graph<AbstractDynamicExplorationGraph<I,V>.Node>) {


    init {
        require(this.degree % 2 == 0) { "Dynamic Exploration Graph (DEG) must be even-regular." }
    }

    /**
     * This method indexes a new entry consisting of an identifier [I] and a vector [V] into this [AbstractDynamicExplorationGraph].
     *
     * @param identifier The identifier [I] of the entry to index.
     * @param vector The vector [V] of the entry to index.
     */
    fun index(identifier: I, vector: V, epsilon: Double) {
        val count = this.size()

        /* Create new (empty) node and store vector. */
        val newNode = Node(identifier)
        this.storeVector(identifier, vector)

        if (count <= this.degree)
        { /* Case 1: Graph does not satisfy regularity condition since it is too small: Create new node and make all existing nodes connect to */
            this.graph.addVertex(newNode)
            for (node in this.graph) {
                val distance = this.distance(vector, node.vector).toFloat()
                if (node != newNode) {
                    this.graph.addEdge(newNode, node, distance)
                    this.graph.addEdge(node, newNode, distance)
                }
            }
        } else { /* Case 2: Graph is not regular. */
            val search = this.search(vector, this.degree, epsilon)
            val connect = HashMap<Node, Float>()
            var skipRng = false

            /* Start insert procedure. */
            while (connect.size < this.degree) {
                val nodesToExplore = search.entries.filter { !connect.contains(it.key) }.associate { it.key to it.value }.toMutableMap()
                while (connect.size < this.degree && nodesToExplore.isNotEmpty()) {
                    var closestNode = nodesToExplore.keys.first()
                    var closestDistance = Double.MAX_VALUE
                    for ((node, _) in nodesToExplore.entries) {
                        val distance = this.distance(vector, node.vector)
                        if (distance < closestDistance) {
                            closestDistance = distance
                            closestNode = node
                        }
                    }
                    nodesToExplore.remove(closestNode)

                    /* Identify the best vertex to connect to existing vertex. */
                    if (skipRng || checkMrng(newNode, connect, closestNode)) {
                        val farthestNodeFromClosest = this.graph.edges(closestNode).filter { !connect.contains(it.key) }.maxBy { it.value }.key
                        connect[closestNode] = this.distance(closestNode.vector, newNode.vector).toFloat()
                        connect[farthestNodeFromClosest] = this.distance(farthestNodeFromClosest.vector, newNode.vector).toFloat()

                        /* Update receiving node. */
                        this.graph.removeEdge(farthestNodeFromClosest, closestNode)
                    }
                }
                skipRng = true
            }

            /* */
            this.graph.addVertex(newNode)
            for ((node, weight) in connect) {
                this.graph.addEdge(newNode, node, weight)
            }
        }
    }

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @param epsilon The epsilon value for the search.
     * @return [List] of [Triple]s containing the [TupleId], distance and [VectorValue] of the approximate nearest neighbours.
     */
    fun search(query: V, k: Int, epsilon: Double): Map<Node,Float> {
        val seed = this.getSeedNodes(this.degree)
        val checked = HashSet<Node>()
        var r = Float.MAX_VALUE

        /* Results. */
        val results = Object2FloatLinkedOpenHashMap<Node>(k + 1)

        /* Perform search. */
        while (seed.isNotEmpty()) {
            /* Find seed node closest to query. */
            var closestNode: Node = seed.first()
            var closestDistance = Double.MAX_VALUE
            for (node in seed) {
                val distance = this.distance(query, node.vector)
                if (distance < closestDistance) {
                    closestDistance = distance
                    closestNode = node
                }
            }
            seed.remove(closestNode)

            /* Abort condition. */
            if (closestDistance > r * (1 + epsilon)) {
                break
            }

            /* Load neighbouring nodes to continue search. */
            for ((node, _) in this.graph.edges(closestNode)) {
                if (!checked.contains(node)) {
                    val distance = this.distance(query, node.vector)
                    if (distance < r * (1 + epsilon)) {
                        seed.add(node)
                        if (distance <= r) {
                            results[node] = distance.toFloat()
                            if (results.size > k) {
                                val largest = results.maxBy { it.value }
                                results.removeFloat(largest.key)
                                r = largest.value
                            }
                        }
                    }

                    /* Add node ID to set of checked nodes. */
                    checked.add(node)
                }
            }
        }

        return results
    }

    /**
     * Returns the size of this [AbstractDynamicExplorationGraph].
     *
     * @return [Long]
     */
    protected abstract fun size(): Long

    /**
     * Loads the vector for the given [TupleId].
     *
     * @param identifier The identifier [I] of the [VectorValue] to load.
     * @return [VectorValue]
     */
    protected abstract fun loadVector(identifier: I): V

    protected abstract fun storeVector(identifier: I, vector: V)

    /**
     * Calculates the distance between two vectors [V]s.
     *
     * @param a The first vector [V]s.
     * @param b The first vector [V]s.
     * @return [Double] distance between the two vectors.
     */
    protected abstract fun distance(a: V, b: V): Double

    /**
     * Obtains random seed [Node]s for range search.
     *
     * @param size The number of seed [Node]s to obtain.
     * @return [MutableMap of [AbstractDynamicExplorationGraph.Node]s keyed by [NodeId]
     */
    private fun getSeedNodes(size: Int): MutableSet<Node> {
        require(size <= this.size()) { "Negative size of $size" }
        val set = HashSet<Node>()
        for ((i, node) in this.graph.withIndex()) {
            if (i % floorDiv(this.graph.size(), size.toLong()) == 0L) {
                set.add(node)
            }
            if (set.size >= size) break
        }
        return set
    }

    /**
     * Tries to identify if the MRNG (Monotonic Relative Neighborhood Graph) condition is satisfied between two [Node]s.
     *
     * @param v1 The first [Node].
     * @param v2 The second [Node].
     * @return True if MRNG condition is satisfied, false otherwise.
     */
    private fun checkMrng(v1: Node, v1N: Map<Node,Float>, v2: Node): Boolean {
        val v2N = this.graph.edges(v2)
        val neighbours = v1N.keys intersect v2N.keys
        val distance = this.distance(v1.vector, v2.vector)
        for (node in neighbours) {
            if (distance > max(v2N[node] ?: 0.0f, v1N[node] ?: 0.0f)) {
                return false
            }
        }
        return true
    }

    /**
     * A [Node] in the [AbstractDynamicExplorationGraph].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Node(val identifier: I) {
        /** The [VectorValue]; this value is loaded lazily. */
        val vector: V by lazy { loadVector(this.identifier) }
        override fun equals(other: Any?): Boolean = other is AbstractDynamicExplorationGraph<*,*>.Node && other.identifier == this.identifier
        override fun hashCode(): Int = this.identifier.hashCode()
    }
}