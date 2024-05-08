package org.vitrivr.cottontail.utilities.graph.undirected

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream

/**
 * An interface for serializing and deserializing vertices of a [WeightedUndirectedXodusGraph].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface VertexSerializer<V> {

    /**
     * Serializes the vertex [V] into a [ByteIterable].
     *
     * @param vertex The vertex [V] to serialize.
     */
    fun serialize(vertex: V): ByteIterable = LightOutputStream().use {
        this.write(vertex, it)
        it.asArrayByteIterable()
    }

    /**
     * Deserializes the vertex [V] from a [ByteIterable].
     *
     * @param iterable The [ByteIterable] to deserialize from
     * @return The vertex [V].
     */
    fun deserialize(iterable: ByteIterable): V = ByteArrayInputStream(iterable.bytesUnsafe).use {
        this.read(it)
    }

    /**
     * Writes the vertex [V] into a [LightOutputStream].
     *
     * @param vertex The vertex [V] to write.
     * @param output The [LightOutputStream] to write to.
     */
    fun write(vertex: V, output: LightOutputStream)

    /**
     * Reads the vertex [V] from a [ByteArrayInputStream].
     *
     * @param input The [ByteArrayInputStream] to read from.
     * @return The vertex [V]
     */
    fun read(input: ByteArrayInputStream): V
}