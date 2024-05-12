package org.vitrivr.cottontail.storage.blob

import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.io.ByteArrayInputStream
import java.io.Closeable
import java.lang.Math.floorDiv
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read

typealias VectorId = Long

/**
 * A [VectorFile] used as a simple, append-only file for [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VectorFile<V: Value>(val path: Path, val type: Types<V>): Closeable {
    /** The [FileChannel] backing this [VectorFile]. */
    private val writeChannel by lazy { FileChannel.open(this.path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND) }

    /** The [FileChannel] backing this [VectorFile]. */
    private val readChannel by lazy {FileChannel.open(this.path, StandardOpenOption.READ) }

    /** A [ByteBuffer] used to write data to this [VectorFile]. */
    private val buffer = ThreadLocal.withInitial {
        ByteBuffer.allocate(this.type.physicalSize).limit(this.type.physicalSize)
    }
    /** The [ValueSerializer] used to serialize and de-serialize entries. */
    private val serializer: ValueSerializer<V> = SerializerFactory.value(this.type)

    /** */
    private val lock = ReentrantReadWriteLock()

    /** The size of this [VectorFile] in number of entries. */
    val size: Long
        get() = floorDiv(this.writeChannel.size(), this.type.physicalSize)

    /**
     * Appends a [VectorValue] to this [VectorFile].
     *
     * @param vectorId The [VectorId] of the [VectorValue] to retrieve.
     * @return [VectorId]
     */
    fun get(vectorId: VectorId): V = this.lock.read  {
        check(this.readChannel.isOpen) { "Failed to write vector; channel closed." }
        val position = vectorId * this.type.physicalSize
        val buffer = this.buffer.get()
        this.readChannel.read(buffer.clear(), position)
        return this.serializer.read(ByteArrayInputStream(buffer.array()))
    }

    /**
     * Appends a [VectorValue] to this [VectorFile].
     *
     * @param vector The [VectorValue] to append.
     * @return [VectorId]
     */
    fun append(vector: V): VectorId = this.lock.read {
        check(this.writeChannel.isOpen) { "Failed to write vector; channel closed." }
        val buffer = this.buffer.get()
        this.serializer.write(LightOutputStream(buffer.array()), vector)
        val position = this.writeChannel.position()
        this.writeChannel.write(buffer.clear())
        return floorDiv(position, this.type.physicalSize)
    }

    /**
     * Flushed the [VectorFile] to disk.
     *
     * @return True on success, false otherwise.
     */
    fun flush() = this.lock.read {
        this.writeChannel.force(true)
    }

    /**
     * Closes this [VectorFile] and the underlying channel.
     */
    override fun close() {
        this.readChannel.close()
        this.writeChannel.close()
    }
}