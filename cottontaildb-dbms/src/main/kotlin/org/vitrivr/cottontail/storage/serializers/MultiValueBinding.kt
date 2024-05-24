package org.vitrivr.cottontail.storage.serializers

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.util.*

/**
 * A facility that can be used to serialize and deserialize a [Tuple] from/to a [ByteBuffer]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class MultiValueBinding(val schema: Array<ColumnDef<*>>) {

    /** [List] if [ValueBinding]s used by this [MultiValueBinding]. */
    @Suppress("UNCHECKED_CAST")
    private val serializers: List<ValueSerializer<Value>> = this.schema.map { ValueSerializer.serializer(it.type) as ValueSerializer<Value> }

    /** */
    private val estimatedSize = this.schema.sumOf { it.type.physicalSize }

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The resulting [Value].
     */
    fun fromEntry(entry: ByteIterable) = this.read(ByteBuffer.wrap(entry.bytesUnsafe, 0, entry.length))

    /**
     * Converts a [Value] to a [ByteIterable].
     *
     * @param value The [Value] to convert.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(value: Tuple): ByteIterable {
        val bytes = ByteArrayOutputStream(this.estimatedSize)
        val output = DataOutputStream(bytes)
        this.write(output, value)
        return ArrayByteIterable(bytes.toByteArray())
    }

    /**
     * Reads a [Tuple] from a [ByteArrayInputStream].
     *
     * @param input The [ByteArrayInputStream] to read.
     * @return The resulting [Tuple].
     */
    fun read(input: ByteBuffer): Tuple {
        val longs = ((this.schema.size -1 ) shr 6) + 1
        val array = LongArray(longs) { input.getLong() }
        val bitset = BitSet.valueOf(array)

        val values = Array(this.schema.size) {
            if (bitset[it]) {
                this.serializers[it].fromBuffer(input)
            } else {
                null
            }
        }

        return StandaloneTuple(0L, this.schema, values)
    }

    /**
     * Writes a [Tuple] to a [LightOutputStream].
     *
     * @param output The [LightOutputStream] to read.
     */
    fun write(output: DataOutputStream, value: Tuple) {
        /* Write null bits. */
        val bits = BitSet(this.schema.size)
        for ((i, v) in value.values().withIndex()) {
            bits.set(i, v != null)
        }
        bits.toLongArray().forEach { output.writeLong(it) }

        /* Write data. */
        for ((i, v) in value.values().withIndex()) {
            if (v != null) {
                val buffer = this.serializers[i].toBuffer(v)
                output.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining())
            }
        }
    }
}