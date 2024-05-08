package org.vitrivr.cottontail.dbms.index.diskann.graph.deg

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.core.basics.CloseableIterator
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Distance
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Node
import org.vitrivr.cottontail.utilities.graph.MutableGraph
import java.util.*
import kotlin.math.max


/**
 * This is an abstract implementation of the [DynamicExplorationGraph], that decouples storage from that basic logic.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractDynamicExplorationGraph<I:Comparable<I>,V>(override val degree: Int, val kExt: Int, val epsilonExt: Float): DynamicExplorationGraph<I,V> {

    /** The [SplittableRandom] to create random numbers to find seed nodes. */
    private val random = SplittableRandom()

    /** The [MutableGraph] backing this [AbstractDynamicExplorationGraph]. */
    protected abstract val graph: MutableGraph<Node<I>>

    /** The size of  this [DynamicExplorationGraph]. */
    override val size: Long
        get() = this.graph.size

    /**
     * This method indexes a new entry consisting of an identifier [I] and a vector [V] into this [AbstractDynamicExplorationGraph].
     *
     * @param identifier The identifier [I] of the entry to index.
     * @param value The value [V] of the entry to index.
     */
    override fun index(identifier: I, value: V) {
        /* Create new (empty) node and store vector. */
        val newNode = Node(identifier)
        this.storeValue(newNode, value)

        if (this.size <= this.degree) { /* Case 1: Graph does not satisfy regularity condition since it is too small; make all existing nodes connect to the new node. */
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
    override fun search(query: V, k: Int, eps: Float): List<Distance<I>> = search(query, k, eps, this.getSeedNodes(10))

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @param eps The epsilon value for the search.
     * @param seeds [List] of seed [Node]s
     * @return [List] of [Triple]s containing the [TupleId], distance and [VectorValue] of the approximate nearest neighbours.
     */
    override fun search(query: V, k: Int, eps: Float, seeds: List<Node<I>>): List<Distance<I>> {
        val checked = ObjectOpenHashSet<Node<I>>()
        val results: TreeSet<Distance<I>> = TreeSet<Distance<I>>()
        var distanceComputationCount = 0

        /* Case 1: Small graph - brute-force search. */
        if (this.size < 1000L) {
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
        for (node in seeds) {
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
     *
     */
    override fun vertices(): CloseableIterator<Node<I>> = this.graph.vertices()

    /**
     *
     */
    override fun edges(from: Node<I>): Map<Node<I>, Float> = this.graph.edges(from)

    /**
     *
     */
    override fun weight(from: Node<I>, to: Node<I>): Float = this.graph.weight(from, to)

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
        val graphSize = this.size
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