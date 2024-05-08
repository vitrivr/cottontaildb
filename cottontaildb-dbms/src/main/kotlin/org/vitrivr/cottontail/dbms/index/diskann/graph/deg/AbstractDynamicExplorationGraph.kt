package org.vitrivr.cottontail.dbms.index.diskann.graph.deg

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndexConfig
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Distance
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Node
import org.vitrivr.cottontail.utilities.graph.Graph
import java.util.*
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
abstract class AbstractDynamicExplorationGraph<I:Comparable<I>,V>(val degree: Int, val kExt: Int, val epsilonExt: Float) {

    /** The [SplittableRandom] to create random numbers to find seed nodes. */
    private val random = SplittableRandom()

    /** The [Graph] backing this [AbstractDynamicExplorationGraph]. */
    protected abstract val graph: Graph<Node<I>>

    /**
     * This method indexes a new entry consisting of an identifier [I] and a vector [V] into this [AbstractDynamicExplorationGraph].
     *
     * @param identifier The identifier [I] of the entry to index.
     * @param value The value [V] of the entry to index.
     */
    fun index(identifier: I, value: V) {
        val count = this.size()

        /* Create new (empty) node and store vector. */
        val newNode = Node(identifier)
        this.storeValue(newNode, value)

        if (count <= this.degree) { /* Case 1: Graph does not satisfy regularity condition since it is too small; make all existing nodes connect to the new node. */
            this.graph.addVertex(newNode)
            for (node in this.graph.vertices()) {
                if (node == newNode) continue
                val distance = this.distance(value, this.getValue(node))
                this.graph.addEdge(node, newNode, distance)
            }
        } else { /* Case 2: Graph satisfies satisfy regularity condition; extend graph by new node. */
            val results = this.search(value, this.kExt, this.epsilonExt, this.getSeedNodes(1))
            var phase = 0

            /* Add new vertex. */
            this.graph.addVertex(newNode)

            /* Start insert procedure. */
            var newNeighbours = this.graph.edges(newNode)
            while (newNeighbours.size < this.degree) {
                for ((candidateLabel, candidateWeight) in results) {
                    val candidateNode = Node(candidateLabel)
                    if (newNeighbours.size >= this.degree) break
                    if (newNeighbours.containsKey(candidateNode)) continue
                    if (phase <= 1 && checkMrng(candidateNode, newNode, candidateWeight)) continue

                    /* Find new neighbour. */
                    val newNeighbour = this.graph.edges(candidateNode).filter { it.key !in newNeighbours }.maxByOrNull { it.value }?.key ?: continue
                    val newNeighbourDistance = this.distance(this.getValue(newNode), this.getValue(newNeighbour))

                    /* Remove edge from candidate node to candidate neighbour. */
                    this.graph.removeEdge(candidateNode, newNeighbour)

                    /* Add edges to new nodes. */
                    this.graph.addEdge(newNode, candidateNode, candidateWeight)
                    this.graph.addEdge(newNode, newNeighbour, newNeighbourDistance)

                    /* Update neighbours. */
                    newNeighbours = this.graph.edges(newNode) /* IMPORTANT: This is needed in case storage does not take place in-memory. */

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
     * @param eps The epsilon value for the search.
     * @return [List] of [Triple]s containing the [TupleId], distance and [VectorValue] of the approximate nearest neighbours.
     */
    fun search(query: V, k: Int, eps: Float): List<Distance<I>> = search(query, k, eps, this.getSeedNodes(10))

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @return [List] of [Triple]s containing the [TupleId], distance and [VectorValue] of the approximate nearest neighbours.
     */
    protected fun search(query: V, k: Int, eps: Float, seed: List<Node<I>>): List<Distance<I>> {
        val checked = ObjectOpenHashSet<Node<I>>()
        val results: TreeSet<Distance<I>> = TreeSet<Distance<I>>()
        var distanceComputationCount = 0

        /* Case 1: Small graph - brute-force search. */
        if (this.size() < 1000L) {
            this.graph.vertices().use { vertices ->
                for (vertex in vertices) {
                    val distance = Distance(vertex.label, this.distance(query, this.getValue(vertex)))
                    distanceComputationCount++
                    results.add(distance)
                    if (results.size > k) {
                        results.pollLast()
                    }
                }
                return results.toList()
            }
        }

        /* Case 2a: DEG search. Initialize queue with results vertices to check. */
        var radius = Float.MAX_VALUE
        val nextNodes = PriorityQueue<Distance<I>>(k * 10)
        for (node in seed) {
            if (!checked.contains(node)) {
                /* Mark node as checked. */
                checked.add(node)

                /* Calculate distance and add to queue. */
                val distance = Distance(node.label, this.distance(query, this.getValue(node)))
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
            val next = nextNodes.poll()

            /* Abort condition. */
            if (next.distance > radius * (1 + eps)) {
                break
            }

            /* Load neighbouring nodes to continue search. */
            for ((node, _) in this.graph.edges(next.asNode())) {
                if (!checked.contains(node)) {
                    /* Mark node as checked. */
                    checked.add(node)

                    /* Calculate distance and add to queue. */
                    val distance = Distance(node.label, this.distance(query, this.getValue(node)))
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
     * Returns the value [V] for the given [Node].
     *
     * @param node The [Node] for which to load the value [V]
     * @return The value [V]
     */
    protected abstract fun getValue(node: Node<I>): V

    /**
     * Stores the value [V] for the given [Node].
     *
     * @param node The [Node] for which to store the value [V]
     * @return The value [V]
     */
    protected abstract fun storeValue(node: Node<I>, value: V)

    /**
     * Calculates the distance between two values [V]s.
     *
     * @param a The first values [V]s.
     * @param b The first values [V]s.
     * @return [Float] distance between the two values.
     */
    protected abstract fun distance(a: V, b: V): Float

    /**
     * Obtains random seed [Node]s for range search.
     *
     * @param sampleSize The number of seed [Node]s to obtain.
     * @return [List] of [Node]s
     */
    private fun getSeedNodes(sampleSize: Int): List<Node<I>> {
        val graphSize = this.graph.size()
        require(sampleSize <= graphSize) { "The sample size $sampleSize exceeds graph size of graph (s = $sampleSize, g = $graphSize)." }
        this.graph.vertices().use { iterator ->
            var position = 0L
            return this.random.longs(0L, graphSize).distinct().limit(sampleSize.toLong()).sorted().mapToObj {
                while (iterator.hasNext()) {
                    if ((position++) == it) {
                        break
                    } else {
                        iterator.next()
                    }
                }
                iterator.next()
            }.toList()
        }
    }

    /**
     * Tries to identify if the MRNG (Monotonic Relative Neighborhood Graph) condition is satisfied between two [Node]s.
     *
     * @param v1 The first [Node].
     * @param v2 The second [Node].
     * @return True if MRNG condition is satisfied, false otherwise.
     */
    private fun checkMrng(v1: Node<I>, v2: Node<I>, targetWeight: Float): Boolean {
        for ((neighbour, neighbourWeight) in this.graph.edges(v1)) {
            val neighbourTargetWeight = this.graph.weight(neighbour, v2)
            if (neighbourTargetWeight >= 0.0f && targetWeight > max(neighbourWeight, neighbourTargetWeight)) {
                return false
            }
        }
        return true
    }
}