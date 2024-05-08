package org.vitrivr.cottontail.dbms.index.diskann.graph

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.utilities.graph.Graph
import java.util.*
import kotlin.math.max
import kotlin.streams.toList


/**
 * This class implements a Dynamic Exploration Graph (DEG) as proposed in [1]. It can be used to perform approximate nearest neighbour search (ANNS).
 *
 * Literature:
 * [1] Hezel, Nico, et al. "Fast Approximate Nearest Neighbor Search with a Dynamic Exploration Graph using Continuous Refinement." arXiv preprint arXiv:2307.10479 (2023)
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractDynamicExplorationGraph<I:Comparable<I>,V>(private val degree: Int, val graph: Graph<AbstractDynamicExplorationGraph<I,V>.Node>, private val epsilonExt: Float = 0.2f, private val kExt: Int = 60) {

    private val random = SplittableRandom()

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

        if (count <= this.degree) { /* Case 1: Graph does not satisfy regularity condition since it is too small: Create new node and make all existing nodes connect to */
            this.graph.addVertex(newNode)
            for (node in this.graph) {
                if (node == newNode) continue
                val distance = this.distance(vector, node.vector)
                this.graph.addEdge(node, newNode, distance)
            }
        } else { /* Case 2: Graph is regular. */
            val results = this.search(vector, this.kExt, this.epsilonExt, this.getSeedNodes(1))
            var phase = 0

            /* Add new vertex. */
            this.graph.addVertex(newNode)

            /* Start insert procedure. */
            val newNeighbours = this.graph.edges(newNode)
            while (newNeighbours.size < this.degree) {
                for ((candidateNode, candidateWeight) in results) {
                    if (newNeighbours.size >= this.degree) break
                    if (newNeighbours.containsKey(candidateNode)) continue
                    if (phase <= 1 && checkMrng(candidateNode, newNode, candidateWeight)) continue

                    /* Find new neighbour. */
                    val newNeighbour = this.graph.edges(candidateNode).filter { it.key !in newNeighbours }.maxByOrNull { it.value }?.key ?: continue
                    val newNeighbourDistance = this.distance(newNode.vector, newNeighbour.vector)

                    /* Remove edge from candidate node to candidate neighbour. */
                    this.graph.removeEdge(candidateNode, newNeighbour)

                    /* Add edges to new nodes. */
                    this.graph.addEdge(newNode, candidateNode, candidateWeight)
                    this.graph.addEdge(newNode, newNeighbour, newNeighbourDistance)
                }
                phase += 1
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
    fun search(query: V, k: Int, eps: Float): List<Distance> = search(query, k, eps, this.getSeedNodes(10))

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @return [List] of [Triple]s containing the [TupleId], distance and [VectorValue] of the approximate nearest neighbours.
     */
    protected fun search(query: V, k: Int, eps: Float, seed: List<Node>): List<Distance> {
        val checked = ObjectOpenHashSet<Node>()
        val results: TreeSet<Distance> = TreeSet<Distance>()
        var distanceComputationCount = 0

        /* Case 1: Small graph - brute-force search. */
        if (this.size() < 1000L) {
            for (vertex in this.graph) {
                val distance = Distance(vertex, this.distance(query, vertex.vector))
                distanceComputationCount++
                results.add(distance)
                if (results.size > k) {
                    results.pollLast()
                }
            }
            return results.toList()
        }

        /* Case 2a: DEG search. Initialize queue with results vertices to check. */
        var radius = Float.MAX_VALUE
        val nextNodes = PriorityQueue<Distance>(k * 10)
        for (node in seed) {
            if (!checked.contains(node)) {
                /* Mark node as checked. */
                checked.add(node)

                /* Calculate distance and add to queue. */
                val distance = Distance(node, this.distance(query, node.vector))
                distanceComputationCount++
                nextNodes.add(distance)
                if (distance.distance < radius) {
                    results.add(distance)
                    if (results.size > k) {
                        results.pollLast()
                        radius = results.last().distance
                    }
                }
            }

        }

        /* Perform based on queue. */
        while (nextNodes.isNotEmpty()) {
            /* Find seed node closest to query. */
            val next: Distance = nextNodes.poll()

            /* Abort condition. */
            if (next.distance > radius * (1 + eps)) {
                break
            }

            /* Load neighbouring nodes to continue search. */
            for ((node, _) in this.graph.edges(next.identifier)) {
                if (!checked.contains(node)) {
                    /* Mark node as checked. */
                    checked.add(node)

                    /* Calculate distance and add to queue. */
                    val distance = Distance(node, this.distance(query, node.vector))
                    distanceComputationCount++
                    if (distance.distance <= radius * (1 + eps)) {
                        /* Add node ID to set of checked nodes. */
                        nextNodes.add(distance)

                        if (distance.distance < radius) {
                            results.add(distance)
                            if (results.size > k) {
                                results.pollLast()
                                radius = results.last().distance
                            }
                        }
                    }
                }
            }
        }

        return results.toList()
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
     * @param sampleSize The number of seed [Node]s to obtain.
     * @return [MutableSet] of [AbstractDynamicExplorationGraph.Node]s
     */
    private fun getSeedNodes(sampleSize: Int): List<Node> {
        val graphSize = this.graph.size()
        require(sampleSize <= graphSize) { "The sample size $sampleSize exceeds graph size of graph (s = $sampleSize, g = $graphSize)" }
        val indexes = this.random.longs(0L, graphSize).distinct().limit(sampleSize.toLong()).sorted().toList().toSet()
        val results = ArrayList<Node>(sampleSize)
        val iterator = this.graph.iterator()
        for (i in 0L until graphSize) {
            val next = iterator.next()
            if (i in indexes) {
                results.add(next)
            }
        }
        return results
    }

    /**
     * Tries to identify if the MRNG (Monotonic Relative Neighborhood Graph) condition is satisfied between two [Node]s.
     *
     * @param v1 The first [Node].
     * @param v2 The second [Node].
     * @return True if MRNG condition is satisfied, false otherwise.
     */
    private fun checkMrng(v1: Node, v2: Node, targetWeight: Float): Boolean {
        for ((neighbour, neighbourWeight) in this.graph.edges(v1)) {
            val neighbourTargetWeight = this.graph.weight(neighbour, v2)
            if (neighbourTargetWeight >= 0.0f && targetWeight > max(neighbourWeight, neighbourTargetWeight)) {
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