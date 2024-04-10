package org.vitrivr.cottontail.storage.serializers.tuples

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.*

/**
 * A facility that can be used to serialize and deserialize a [Tuple] from/to a [ByteBuffer]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class TupleSerializer(val schema: Array<ColumnDef<*>>) {

    /** [List] if [ValueSerializer]s used by this [TupleSerializer]. */
    private val serializers: List<ValueSerializer<Value>> = this.schema.map { SerializerFactory.value(it.type) as ValueSerializer<Value> }

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The resulting [Value].
     */
    fun fromEntry(entry: ByteIterable) = this.read(ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length))

    /**
     * Converts a [Value] to a [ByteIterable].
     *
     * @param value The [Value] to convert.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(value: Tuple): ByteIterable {
        val output = LightOutputStream(this.schema.sumOf { it.type.physicalSize })
        this.write(output, value)
        return output.asArrayByteIterable()
    }

    /**
     * Reads a [Tuple] from a [ByteArrayInputStream].
     *
     * @param input The [ByteArrayInputStream] to read.
     * @return The resulting [Tuple].
     */
    fun read(input: ByteArrayInputStream): Tuple {
        val longs = ((this.schema.size-1) shr 6) + 1
        val array = LongArray(longs) { LongBinding.BINDING.readObject(input) }
        val bitset = BitSet.valueOf(array)

        val values = Array(this.schema.size) {
            if (bitset[it]) {
                this.serializers[it].read(input)
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
    fun write(output: LightOutputStream, value: Tuple) {
        /* Write null bits. */
        val bits = BitSet(this.schema.size)
        for ((i, v) in value.values().withIndex()) {
            bits.set(i, v != null)
        }
        bits.toLongArray().forEach { LongBinding.BINDING.writeObject(output, it) }

        /* Write data. */
        for ((i, v) in value.values().withIndex()) {
            if (v != null) {
                this.serializers[i].write(output, v)
            }
        }
    }
}