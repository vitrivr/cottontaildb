package org.vitrivr.cottontail.dbms.index.diskann.graph

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap
import jetbrains.exodus.core.dataStructures.hash.LongHashSet
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.VectorValue
import java.util.*
import kotlin.math.max

typealias NodeId = Long

typealias Weight = Double

/**
 * This class implements a Dynamic Exploration Graph (DEG) as proposed in [1]. It can be used to perform approximate nearest neighbour search (ANNS).
 *
 * Literature:
 * [1] Hezel, Nico, et al. "Fast Approximate Nearest Neighbor Search with a Dynamic Exploration Graph using Continuous Refinement." arXiv preprint arXiv:2307.10479 (2023)
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractDynamicExplorationGraph<I,V>(val degree: Int): Iterable<Pair<NodeId,AbstractDynamicExplorationGraph<I,V>.Node>> {


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
        val newNodeId = count + 1
        val newNode = Node(identifier, Long2ObjectArrayMap(this.degree))


        if (size() <= this.degree + 1)  { /* Case 1: Graph satisfies regularity condition: Create new node and make all existing nodes connect to */
            for ((nodeId, node) in this) {
                val distance = this.distance(vector, node.vector)
                node.addEdge(newNodeId, distance)
                newNode.addEdge(nodeId, distance)
            }
        } else { /* Case 2: Graph is not regular. */
            val seed = this.getSeedNodes()
            val nearest = this.search(vector, this.degree, epsilon)
            var skipRng = false

            /* Start insert procedure (. */
            while (nearest.size < this.degree) {
                val b = seed.entries.filter { !nearest.containsKey(it.key) }.associate { it.key to it.value }.toMutableMap()
                while (nearest.size < this.degree && b.isNotEmpty()) {
                    var closestNodeId = b.keys.first()
                    var closestNode: Node = b.values.first()
                    var closestDistance = Double.MAX_VALUE
                    for ((nodeId, node) in b) {
                        val distance = this.distance(vector, node.vector)
                        if (distance < closestDistance) {
                            closestDistance = distance
                            closestNodeId = nodeId
                            closestNode = node
                        }
                        b.remove(closestNodeId)
                        if (skipRng || checkMrng(newNode, closestNode)) {
                             /* TODO */
                        }
                    }
                }
                skipRng = true
            }
        }

        /* Store new node. */
        this.storeNode(newNodeId, newNode)
    }

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @param epsilon The epsilon value for the search.
     * @return [List] of [Triple]s containing the [TupleId], distance and [VectorValue] of the approximate nearest neighbours.
     */
    fun search(query: V, k: Int, epsilon: Double): Map<NodeId,Pair<Node,Double>> {
        val seed = this.getSeedNodes()
        val checked = LongHashSet()
        var r = Double.MAX_VALUE

        /* Results. */
        val results = Long2ObjectArrayMap<Pair<Node,Double>>(k + 1)

        /* Perform search. */
        while (seed.isNotEmpty()) {
            /* Find seed node closest to query. */
            var closestNodeId = seed.keys.first()
            var closestNode: Node = seed.values.first()
            var closestDistance = Double.MAX_VALUE
            for ((id, node) in seed) {
                val distance = this.distance(query, node.vector)
                if (distance < closestDistance) {
                    closestDistance = distance
                    closestNodeId = id
                    closestNode = node
                }
            }
            seed.remove(closestNodeId)

            /* Abort condition. */
            if (closestDistance > r * (1 + epsilon)) {
                break
            }

            /* Load neighbouring nodes to continue search. */
            for ((nodeId, _) in closestNode.neighbours) {
                if (!checked.contains(nodeId)) {
                    val node = this.getNode(nodeId)
                    val distance = this.distance(query, node.vector)
                    if (distance < r * (1 + epsilon)) {
                        seed[nodeId] = node
                        if (distance <= r) {
                            results[nodeId] = node to distance
                            if (results.size > k) {
                                val largest = results.long2ObjectEntrySet().maxBy { it.value.second }
                                results.remove(largest.longKey)
                                r = largest.value.second
                            }
                        }
                    }

                    /* Add node ID to set of checked nodes. */
                    checked.add(nodeId)
                }
            }
        }

        return results
    }

    /**
     * Returns a [NodeIterator] over the [Node]s in this [AbstractDynamicExplorationGraph].
     *
     * The default implementation may not be ideal, depending on what underlying storage is used.
     *
     * @return [NodeIterator]
     */
    override fun iterator(): Iterator<Pair<NodeId,Node>> = NodeIterator()

    /**
     * Stores the [Node] with the given [NodeId]
     *
     * @param nodeId The [NodeId] of the [Node] to return.
     * @param node The [Node] to store.
     * @throws NoSuchElementException If [Node] with [NodeId] doesn't exist.
     */
    protected abstract fun storeNode(nodeId: NodeId, node: Node)

    /**
     * Returns the  [Node]  with the given [NodeId]
     *
     * @param nodeId The [NodeId] of the [Node] to return.
     * @return [Node]
     * @throws NoSuchElementException If [Node] with [NodeId] doesn't exist.
     */
    protected abstract fun getNode(nodeId: NodeId): Node

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
    private fun getSeedNodes(size: Int = 10): MutableMap<NodeId, Node> {
        val map = Long2ObjectArrayMap<Node>()
        val random = SplittableRandom()
        (0 until size).map {
            while (true) {
                val nextNodeId = random.nextLong(0L, this.size())
                val nextNode = this.getNode(nextNodeId)
                if (map.putIfAbsent(nextNodeId, nextNode) != null) {
                    break
                }
            }
        }
        return map
    }

    /**
     * Tries to identify if the MRNG (Monotonic Relative Neighborhood Graph) condition is satisfied between two [Node]s.
     *
     * @param v1 The first [Node].
     * @param v2 The second [Node].
     * @return True if MRNG condition is satisfied, false otherwise.
     */
    private fun checkMrng(v1: Node, v2: Node): Boolean {
        val neighbours = v1.neighbours.keys.intersect(v2.neighbours.keys)
        val distance = this.distance(v1.vector, v2.vector)
        for (nodeId in neighbours) {
            if (distance > max(v2.neighbours[nodeId] ?: 0.0, v1.neighbours[nodeId] ?: 0.0)) {
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
    inner class Node(val identifier: I, private val _edges: MutableMap<NodeId,Weight>) {
        /** The [VectorValue]; this value is loaded lazily. */
        val vector: V by lazy { loadVector(this.identifier) }

        /** The neighbours of this [Node]. */
        val neighbours: Map<NodeId,Weight>
            get() = this._edges.toMap()

        /**
         * Adds a new edge to this [Node].
         *
         * @param nodeId The [NodeId] of the
         */
        fun addEdge(nodeId: NodeId, weight: Weight) {
            require(this._edges.size < this@AbstractDynamicExplorationGraph.degree) { "Node contains to many edges (maximum degree is ${this@AbstractDynamicExplorationGraph.degree})." }
            require(nodeId > 0 && nodeId < this@AbstractDynamicExplorationGraph.size()) { "NodeId $nodeId is out-of-bounds (maximum size = ${size()})." }
            this._edges[nodeId] = weight
        }

        /**
         * Removes an edge from this [Node].
         *
         * @param nodeId The [NodeId] of the edge to remove.
         */
        fun removeEdge(nodeId: NodeId) {
            this._edges.remove(nodeId)
        }
    }

    /**
     * Returns an [Iterator] over the [Node]s in this [AbstractDynamicExplorationGraph].
     *
     * <strong>Important:</string> This is a fairly naive implementation that could be improved in concrete implementations.
     */
    inner class NodeIterator: Iterator<Pair<NodeId,Node>> {
        private var current: NodeId = 0L
        override fun hasNext(): Boolean = this.current < this@AbstractDynamicExplorationGraph.size()
        override fun next(): Pair<NodeId,Node> = this.current to this@AbstractDynamicExplorationGraph.getNode(this.current++)
    }
}