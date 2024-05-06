package org.vitrivr.cottontail.dbms.index.diskann.graph

import it.unimi.dsi.fastutil.objects.Object2FloatLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectArraySet
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.utilities.graph.Graph
import java.lang.Math.floorDiv
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
abstract class AbstractDynamicExplorationGraph<I:Comparable<I>,V>(private val degree: Int, val graph: Graph<AbstractDynamicExplorationGraph<I,V>.Node>, private val epsilonExt: Float = 0.3f, private val kExt: Int = 60) {


    init {
        require(this.degree % 2 == 0) { "Dynamic Exploration Graph (DEG) must be even-regular." }
    }

    /**
     * This method indexes a new entry consisting of an identifier [I] and a vector [V] into this [AbstractDynamicExplorationGraph].
     *
     * @param identifier The identifier [I] of the entry to index.
     * @param vector The vector [V] of the entry to index.
     */
    fun index(identifier: I, vector: V) {
        val count = this.size()

        /* Create new (empty) node and store vector. */
        val newNode = Node(identifier)
        this.storeVector(identifier, vector)
        this.graph.addVertex(newNode)

        if (count <= this.degree) { /* Case 1: Graph does not satisfy regularity condition since it is too small: Create new node and make all existing nodes connect to */
            for (node in this.graph) {
                if (node == newNode) continue
                val distance = this.distance(vector, node.vector)
                this.graph.addEdge(newNode, node, distance)
                this.graph.addEdge(node, newNode, distance)
            }
        } else { /* Case 2: Graph is regular. */
            val results = this.search(vector, this.kExt, this.epsilonExt)
            var skipRng = false

            /* Start insert procedure. */
            var newNeighbours = this.graph.edges(newNode)
            while (newNeighbours.size < this.degree) {
                for ((candidateNode, candidateWeight) in results) {
                    if (newNeighbours.size >= this.degree) break
                    if (newNeighbours.contains(candidateNode)) continue
                    if (!(skipRng || checkMrng(newNode, candidateNode, candidateWeight))) continue

                    /* Find candidate neighbour. */
                    val (candidateNeighbour,candidateNeighbourWeight) = this.graph.edges(candidateNode).filter { !newNeighbours.contains(it.key) }.maxBy { it.value }

                    /* Remove edge from candidate node to candidate neighbour. */
                    this.graph.removeEdge(candidateNode, candidateNeighbour)

                    /* Add edges to new nodes. */
                    this.graph.addEdge(newNode, candidateNode, candidateWeight)
                    this.graph.addEdge(newNode, candidateNeighbour, candidateNeighbourWeight)
                }
                skipRng = true
                newNeighbours = this.graph.edges(newNode) /* Fetch new neighbours. */
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
    fun search(query: V, k: Int, epsilon: Float): List<Distance> {
        val seed = this.getSeedNodes(this.degree)
        val checked = HashSet<Node>()
        var r = Float.MAX_VALUE

        /* Results. */
        val results = Object2FloatLinkedOpenHashMap<Node>(k + 1)

        /* Perform search. */
        while (seed.isNotEmpty()) {
            /* Find seed node closest to query. */
            var closestNode: Node = seed.first()
            var closestDistance = Float.MAX_VALUE
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
                            results[node] = distance
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

        return results.map { Distance(it.key, it.value) }.sorted()
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
     * @return [Float] distance between the two vectors.
     */
    protected abstract fun distance(a: V, b: V): Float

    /**
     * Obtains random seed [Node]s for range search.
     *
     * @param size The number of seed [Node]s to obtain.
     * @return [MutableSet] of [AbstractDynamicExplorationGraph.Node]s
     */
    private fun getSeedNodes(size: Int): MutableSet<Node> {
        val graphSize = this.graph.size()
        val sampleSize = size.toLong()
        require(sampleSize <= graphSize) { "The sample size $sampleSize exceeds graph size of graph (s = $sampleSize, g = $graphSize)" }
        val set = ObjectArraySet<Node>(size)
        for ((i, node) in this.graph.withIndex()) {
            if (i % floorDiv(graphSize, sampleSize) == 0L) {
                set.add(node)
                if (set.size >= size) break
            }
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
    private fun checkMrng(v1: Node, v2: Node, targetWeight: Float): Boolean {
        val v1N = this.graph.edges(v1)
        val v2N = this.graph.edges(v2)
        for (node in (v1N.keys intersect v2N.keys)) {
            if (targetWeight > max(v2N[node]!!, v1N[node]!!)) {
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
    inner class Node(val identifier: I): Comparable<Node> {
        /** The [VectorValue]; this value is loaded lazily. */
        val vector: V by lazy { loadVector(this.identifier) }
        override fun compareTo(other: Node): Int = this.identifier.compareTo(other.identifier)
        override fun equals(other: Any?): Boolean = other is AbstractDynamicExplorationGraph<*,*>.Node && other.identifier == this.identifier
        override fun hashCode(): Int = this.identifier.hashCode()
    }

    /**
     * A [Distance] element produced by this [AbstractDynamicExplorationGraph].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Distance(val identifier: Node, val distance: Float): Comparable<Distance> {
        override fun compareTo(other: Distance): Int {
            val result = this.distance.compareTo(other.distance)
            return if (result == 0) {
                this.identifier.compareTo(other.identifier)
            } else {
                result
            }
        }

        operator fun component1(): Node = this.identifier
        operator fun component2(): Float = this.distance

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is AbstractDynamicExplorationGraph<*,*>.Distance) return false
            if (this.distance != other.distance) return false
            if (this.identifier != other.identifier) return false
            return true
        }

        override fun hashCode(): Int {
            var result = identifier.hashCode()
            result = 31 * result + distance.hashCode()
            return result
        }
    }
}