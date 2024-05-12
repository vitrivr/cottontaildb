package org.vitrivr.cottontail.utilities.graph.undirected

import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.basics.CloseableIterator
import org.vitrivr.cottontail.utilities.graph.MutableGraph
import java.io.ByteArrayInputStream

/**
 * An undirected, in-memory implementation of the [MutableGraph] interface.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class WeightedUndirectedInMemoryGraph<V>: MutableGraph<V> {

    /** An [Object2ObjectLinkedOpenHashMap] used as adjacency list for this [WeightedUndirectedInMemoryGraph]. */
    private val adjacencyList = Object2ObjectOpenHashMap<V, Object2FloatOpenHashMap<V>>()

    /** The number of vertices in this [WeightedUndirectedInMemoryGraph]. */
    override val size: Int
        get() = this.adjacencyList.size

    /**
     * Returns an unmodifiable [Map] of all edges from the given Vertex [V] in this [WeightedUndirectedInMemoryGraph].
     *
     * @return [Map] of all edges from [V] in this [WeightedUndirectedInMemoryGraph].
     */
    override fun edges(from: V): Map<V,Float> = this.adjacencyList[from] ?: throw NoSuchElementException("The vertex $from does not exist in the graph.")

    /**
     * Adds a new vertex of type [V] to this [WeightedUndirectedInMemoryGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    override fun addVertex(v: V): Boolean {
        if (!this.adjacencyList.containsKey(v)) {
            val edges = Object2FloatOpenHashMap<V>()
            edges.defaultReturnValue(Float.MIN_VALUE)
            this.adjacencyList[v] = edges
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
        val edges = this.adjacencyList.remove(v) ?: return false
        edges.keys.forEach { this.adjacencyList[it]?.removeFloat(v) }
        return true
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
        /* Sanity checks. */
        require(from != to) { "Failed to add edge: FROM and TO vertex are the same."}
        val e1 = this.adjacencyList[from] ?: throw NoSuchElementException("Failed to add edge: FROM vertex $from does not exist in the graph." )
        val e2 = this.adjacencyList[to] ?: throw NoSuchElementException("Failed to add edge: TO vertex $to does not exist in the graph." )

        /* Add edge if it does not exist. */
        if (!e1.containsKey(to) && !e2.containsKey(from)) {
            e1[to] = weight
            e2[from] = weight
            return true
        }
        return false
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
        if (e1.containsKey(to) && e2.containsKey(from)) {
            e1.removeFloat(to)
            e2.removeFloat(from)
            return true
        }
        return false
    }

    /**
     * Checks if this [WeightedUndirectedInMemoryGraph] contains an edge from vertex [V] to vertex [V].
     */
    override fun hasEdge(from: V, to: V): Boolean = this.weight(from, to) != Float.MIN_VALUE

    /**
     *
     */
    override fun weight(from: V, to: V): Float {
        val e = this.adjacencyList[from] ?: return Float.MIN_VALUE
        return e.getFloat(to)
    }

    /**
     *
     */
    override fun vertices(): Iterator<V> = this@WeightedUndirectedInMemoryGraph.adjacencyList.keys.iterator()

    /**
     * Writes the contents of this [WeightedUndirectedInMemoryGraph] to the provided [Store] using the provided [Transaction].
     */
    fun writeToStore(store: Store, txn: Transaction, serializer: VertexSerializer<V>) {
        require(store.config == StoreConfig.WITH_DUPLICATES) { "A weighted undirected in-memory graph can only be written to a store with duplicates enabled."}
        val out = LightOutputStream()
        for ((vertex, edges) in this.adjacencyList) {
            out.clear()
            val key = serializer.serialize(vertex)
            store.delete(txn, key)
            if (edges.isEmpty()) {
                store.add(txn, key, ByteIterable.EMPTY)
            } else {
                for ((edge, weight) in edges) {
                    serializer.write(edge, out)
                    FloatBinding.BINDING.writeObject(out, weight)
                    store.put(txn, key, out.asArrayByteIterable())
                }
            }
        }
    }

    /**
     *
     */
    fun readFromStore(store: Store, txn: Transaction, serializer: VertexSerializer<V>) {
        require(store.config == StoreConfig.WITH_DUPLICATES) { "A weighted undirected in-memory graph can only be written to a store with duplicates enabled." }
        this.adjacencyList.clear()
        store.openCursor(txn).use {
            while (it.nextNoDup) {
                val key = serializer.deserialize(it.key)
                val edges = Object2FloatOpenHashMap<V>()
                if (it.value != ByteIterable.EMPTY) {
                    do {
                        val input = ByteArrayInputStream(it.value.bytesUnsafe)
                        val to = serializer.read(input)
                        val weight = FloatBinding.BINDING.readObject(input).toFloat()
                        edges[to] = weight
                    } while (it.nextDup)
                    this.adjacencyList[key] = edges
                }
            }
        }
    }
}