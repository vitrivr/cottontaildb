package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FloatVectorValueValueSerializer(size: Int): ValueSerializer<FloatVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<FloatVectorValue> = Types.FloatVector(size)
    override fun fromEntry(entry: ByteIterable): FloatVectorValue = FloatVectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
    override fun toEntry(value: FloatVectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}