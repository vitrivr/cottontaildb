package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.HalfVectorValue
import org.vitrivr.cottontail.utilities.math.Half
import org.xerial.snappy.Snappy
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization with 16 bit precision.
 */
class HalfVectorValueValueSerializer(size: Int) : ValueSerializer<HalfVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<HalfVectorValue> = Types.HalfVector(size)
    override fun fromEntry(entry: ByteIterable): HalfVectorValue {
        val buffer = ByteBuffer.wrap(Snappy.uncompress(entry.bytesUnsafe)).asShortBuffer()
        val floats = FloatArray(buffer.remaining()) { Half(buffer[it].toUShort()).toFloat() }
        return HalfVectorValue(floats)
    }

    override fun toEntry(value: HalfVectorValue): ByteIterable {
        val halfs = ShortArray(value.data.size) { Half(value.data[it]).toShort() }
        val compressed = Snappy.compress(halfs)
        return ArrayByteIterable(compressed, compressed.size)
    }
}