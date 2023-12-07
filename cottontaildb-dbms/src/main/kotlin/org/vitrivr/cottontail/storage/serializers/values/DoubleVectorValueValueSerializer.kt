package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DoubleVectorValueValueSerializer(size: Int): ValueSerializer<DoubleVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<DoubleVectorValue> = Types.DoubleVector(size)
    override fun fromEntry(entry: ByteIterable): DoubleVectorValue = DoubleVectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
    override fun toEntry(value: DoubleVectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}