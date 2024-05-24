package org.vitrivr.cottontail.storage.serializers

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteBufferByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import java.io.DataOutput
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * A [ValueBinding] is convert [Value]s into [ByteIterable]s and vice-versa.
 *
 * This is used to support Xodus storage.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ValueBinding<T: Value>(val serializer: ValueSerializer<T>) {
    /** Internal buffer used for deserialization. */
    private var buffer = ByteBuffer.allocate(max(this.serializer.type.physicalSize, 1))

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return  [Value] [T]
     */
    fun fromEntry(entry: ByteIterable): T {
        this.buffer.clear()
        if (this.buffer.remaining() < entry.length) {
            this.buffer = ByteBuffer.allocate(entry.length)
        }
        for (b in entry) this.buffer.put(b)
        return this.serializer.fromBuffer(this.buffer.flip())
    }

    /**
     * Converts a [Value] of type [T] to a [ByteIterable].
     *
     * @return [ByteBufferByteIterable]
     */
    fun toEntry(value: T): ByteIterable {
        val buffer = this.serializer.toBuffer(value)
        return ArrayByteIterable(buffer.array(), buffer.remaining())
    }

    /**
     * Reads a [Value] of type [T] from a [ByteBuffer].
     *
     * @param buffer The [ByteBuffer] to read from.
     * @return [T]
     */
    fun read(buffer: ByteBuffer): T = this.serializer.fromBuffer(buffer)

    /**
     * Reads a [Value] of type [T] from a [ByteBuffer].
     *
     * @param output The [DataOutput] to write to.
     * @return value The [Value] [T] to write
     */
    fun write(output: DataOutput, value: T) {
        val buffer = this.serializer.toBuffer(value)
        output.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining())
    }
}