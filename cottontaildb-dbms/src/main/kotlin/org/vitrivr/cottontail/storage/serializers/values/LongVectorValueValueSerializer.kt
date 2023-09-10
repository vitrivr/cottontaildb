package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LongVectorValueValueSerializer(val size: Int): ValueSerializer<LongVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<LongVectorValue> = Types.LongVector(this.size)

    override fun fromEntry(entry: ByteIterable): LongVectorValue = LongVectorValue(Snappy.uncompressLongArray(entry.bytesUnsafe))

    override fun toEntry(value: LongVectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}