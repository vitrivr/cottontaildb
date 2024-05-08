package org.vitrivr.cottontail.utilities.graph.undirected

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.utilities.graph.Graph
import org.vitrivr.cottontail.utilities.graph.NodeSerializer
import java.io.ByteArrayInputStream

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class UndirectedXodusGraph<V>(private val store: Store, private val txn: Transaction, private val serializer: NodeSerializer<V>, val maxDegree: Int = Int.MAX_VALUE): Graph<V> {


    private var count: Long = 0L

    init {
        this.store.openCursor(this.txn.readonlySnapshot).use { cursor ->
            while (cursor.nextNoDup) this.count++
        }
    }

    /**
     * Returns the number of vertexes in this [UndirectedXodusGraph].
     *
     * @return Number of vertexes in this [UndirectedXodusGraph].
     */
    override fun size(): Long = this.count

    /**
     * Adds a new vertex of type [V] to this [UndirectedXodusGraph].
     *
     * @param v The vertex [V] to add.
     * @return True on success, false otherwise.
     */
    override fun addVertex(v: V): Boolean {
        if (this.store.add(this.txn, this.serializer.serialize(v), ByteIterable.EMPTY)) {
            this.count++
            return true
        } else {
            return false
        }
    }

    /**
     * Removes a vertex of type [V] from this [UndirectedXodusGraph] (and all associated edges).
     *
     * @param v The vertex [V] to remove.
     * @return True on success, false otherwise.
     */
    override fun removeVertex(v: V): Boolean = this.store.delete(this.txn, this.serializer.serialize(v))

    /**
     * Adds an edge between two vertices to this [Graph]
     *
     * @param from The vertex [V] to start the edge at.
     * @param to The vertex [V] to end the edg at.
     * @param weight The weight of the edge.
     * @return True on success, false otherwise.
     */
    override fun addEdge(from: V, to: V, weight: Float): Boolean {
        if (this.store.exists(this.txn, this.serializer.serialize(from), ByteIterable.EMPTY)) {
            this.store.delete(this.txn, this.serializer.serialize(from))
        }
        return this.store.put(this.txn, this.serializer.serialize(from), this.serializeEdge(to to weight)) &&
            this.store.put(this.txn, this.serializer.serialize(to), this.serializeEdge(from to weight))
    }

    /**
     * Removes an edge between two vertices to this [Graph]
     *
     * @param from The start vertex [V].
     * @param to The end vertex [V].
     * @return True on success, false otherwise.
     */
    override fun removeEdge(from: V, to: V): Boolean = this.store.openCursor(this.txn).use { cursor ->
       val fromRaw = this.serializer.serialize(from)
       val toRaw = this.serializer.serialize(to)
       if (cursor.getSearchBoth(fromRaw, toRaw)) {
           cursor.deleteCurrent()
           if (!cursor.getSearchBoth(toRaw, fromRaw)) throw IllegalStateException("Missing counterpart.")
           cursor.deleteCurrent()
           true
       } else {
           false
       }
    }

    /**
     * Returns an unmodifiable [Map] of all edges from the given Vertex [V] in this [UndirectedXodusGraph].
     *
     * @param from The vertex [V] to get edges from.
     * @return [Map] of all edges from [V] in this [UndirectedXodusGraph].
     */
    override fun edges(from: V): Map<V, Float> = this.store.openCursor(this.txn).use { cursor ->
        val results = mutableMapOf<V,Float>()
        if (cursor.getSearchKey(this.serializer.serialize(from)) != null) {
           do {
               val edge = this.deserializeEdge(cursor.value)
               results[edge.first] = edge.second
           } while (cursor.nextDup)
        }
        results
    }

    override fun vertices(): Collection<V> {
        TODO("Not yet implemented")
    }

    override fun weight(from: V, to: V): Float {
        TODO("Not yet implemented")
    }

    override fun iterator(): Iterator<V> {
        TODO("Not yet implemented")
    }

    /**
     *
     */

    private fun serializeEdge(edge: Pair<V, Float>): ByteIterable {
        val out = LightOutputStream()
        this.serializer.write(edge.first, out)
        FloatBinding.BINDING.writeObject(out, edge.second)
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