package org.vitrivr.cottontail.utilities.graph.undirected

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.basics.CloseableIterator
import org.vitrivr.cottontail.utilities.graph.MutableGraph
import java.io.ByteArrayInputStream
import java.util.LinkedList

/**
 * An weighted, undirected implementation of the [MutableGraph] interface that stores its data in an Xodus [Store].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class WeightedUndirectedXodusGraph<V>(private val store: Store, private val txn: Transaction, private val serializer: VertexSerializer<V>): MutableGraph<V> {

    /** The number of vertices in this [WeightedUndirectedXodusGraph]. */
    override var size: Long = 0L
        private set

    init {

        this.store.openCursor(this.txn).use { cursor ->
            while (cursor.nextNoDup) this.size++
        }
    }

    /**
     * Adds a new vertex of type [V] to this [WeightedUndirectedXodusGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    override fun addVertex(v: V): Boolean {
        if (this.store.add(this.txn, this.serializer.serialize(v), ByteIterable.EMPTY)) {
            this.size++
            return true
        } else {
            return false
        }
    }

    /**
     * Removes a vertex of type [V] from this [WeightedUndirectedXodusGraph] (and all associated edges).
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    override fun removeVertex(v: V): Boolean {
        val edgesRaw = LinkedList<ByteIterable>()
        val vertexRaw = this.serializer.serialize(v)
        this.store.openCursor(this.txn).use {
            /* Remove all edges FROM the deleted vertex. */
            it.getSearchKey(vertexRaw) ?: return false
            do {
                edgesRaw.add(it.value)
                it.deleteCurrent()
            } while (it.nextDup)

            /* Remove all edges TO the deleted vertex. */
            for (edgeRaw in edgesRaw) {
                it.getSearchBothRange(edgeRaw, vertexRaw) ?: throw IllegalArgumentException("Could not find key.")
                it.deleteCurrent()
            }
        }

        /* Decrement counter. */
        this.size--
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
        /* Sanity check. */
        require(from != to) { "Failed to add edge: FROM and TO vertex are the same."}
        val fromRaw = this.serializer.serialize(from)
        val toRaw = this.serializer.serialize(to)

        /* Fetch existing vertices. */
        val fromExisting = this.store.get(this.txn, fromRaw) ?: throw NoSuchElementException("Failed to add edge: FROM vertex $from does not exist in the graph.")
        val toExisting = this.store.get(this.txn, toRaw) ?: throw NoSuchElementException("Failed to add edge: TO vertex $to does not exist in the graph.")

        /* Serialize FROM -> TO edge. */
        if (fromExisting == ByteIterable.EMPTY) {
            this.store.delete(this.txn, fromRaw)
        }
        if (!this.store.put(this.txn, fromRaw, this.serializeEdge(to, weight))) {
            return false /* Edge already exists. */
        }

        /* Serialize TO -> FROM edge. */
        if (toExisting == ByteIterable.EMPTY) {
            this.store.delete(this.txn, toRaw)
        }
        if (!this.store.put(this.txn, toRaw, this.serializeEdge(from, weight))) {
            throw IllegalStateException("Graph corrupted: Inserting TO -> FROM edge failed.")
        }
        return true
    }

    /**
     * Removes an edge between two vertices to this [MutableGraph]
     *
     * @param from The start vertex [V].
     * @param to The end vertex [V].
     * @return True on success, false otherwise.
     */
    override fun removeEdge(from: V, to: V): Boolean {
        val fromRaw = this.serializer.serialize(from)
        val toRaw = this.serializer.serialize(to)

        /* Fetch existing vertices. */
        this.store.get(this.txn, fromRaw) ?: throw NoSuchElementException("FROM vertex $from does not exist.")
       this.store.get(this.txn, toRaw) ?: throw NoSuchElementException("TO vertex $to does not exist.")

        /* Remove edge from both vertices. */
        this.store.openCursor(this.txn).use { cursor ->
            cursor.getSearchBothRange(fromRaw, toRaw) ?: return false
            if (cursor.deleteCurrent()) {
                cursor.getSearchBothRange(toRaw, fromRaw) ?: throw IllegalStateException("Graph corrupted: Deleting TO -> FROM edge failed.")
                if (!cursor.deleteCurrent()) {
                    throw IllegalStateException("Graph corrupted: Deleting TO -> FROM edge failed.")
                }
            }
        }

        /* Add placeholder, if last entry was deleted. */
        if (this.store.get(this.txn, fromRaw) == null) {
            this.store.add(this.txn, fromRaw, ByteIterable.EMPTY)
        }

        /* Add placeholder, if last entry was deleted. */
        if (this.store.get(this.txn, toRaw) == null) {
            this.store.add(this.txn, toRaw, ByteIterable.EMPTY)
        }

        return true
    }

    /**
     * Returns an unmodifiable [Map] of all edges from the given Vertex [V] in this [WeightedUndirectedXodusGraph].
     *
     * @param from The vertex [V] to get edges from.
     * @return [Map] of all edges from [V] in this [WeightedUndirectedXodusGraph].
     */
    override fun edges(from: V): Map<V, Float> = this.store.openCursor(this.txn).use { cursor ->
        val entry = cursor.getSearchKey(this.serializer.serialize(from)) ?: throw NoSuchElementException("The vertex $from does not exist in the graph.")
        if (entry == ByteIterable.EMPTY) return emptyMap()
        val results = mutableMapOf<V,Float>()
        do {
            val edge = this.deserializeEdge(cursor.value)
            results[edge.first] = edge.second
        } while (cursor.nextDup)
        results
    }

    /**
     *
     */
    override fun weight(from: V, to: V): Float = this.store.openCursor(this.txn).use { cursor ->
        val fromRaw = this.serializer.serialize(from)
        val toRaw = this.serializer.serialize(to)
        val entry = cursor.getSearchBothRange(fromRaw, toRaw) ?: return Float.MIN_VALUE
        if (entry == ByteIterable.EMPTY)  return Float.MIN_VALUE
        this.deserializeEdge(entry).second
    }

    /**
     * Returns a [CloseableIterator] over vertices [V] for this [WeightedUndirectedXodusGraph].
     *
     * @return [CloseableIterator]
     */
    override fun vertices(): CloseableIterator<V> = object: CloseableIterator<V> {
        private val cursor = this@WeightedUndirectedXodusGraph.store.openCursor(this@WeightedUndirectedXodusGraph.txn)
        override fun hasNext(): Boolean = this.cursor.nextNoDup
        override fun next(): V = this@WeightedUndirectedXodusGraph.serializer.deserialize(this.cursor.key)
        override fun close() = this.cursor.close()
    }

    /**
     *
     */
    private fun serializeEdge(edge: V, weight: Float): ByteIterable {
        val out = LightOutputStream()
        this.serializer.write(edge, out)
        FloatBinding.BINDING.writeObject(out, weight)
        return out.asArrayByteIterable()
    }

    /**
     *
     */

    private fun deserializeEdge(iterable: ByteIterable): Pair<V, Float> {
        val input = ByteArrayInputStream(iterable.bytesUnsafe)
        return this.serializer.read(input) to FloatBinding.BINDING.readObject(input)
    }
}