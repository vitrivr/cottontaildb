package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.ShortVectorValue
import org.xerial.snappy.Snappy
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [ShortVectorValue] serialization and deserialization.
 *
 */
class ShortVectorValueValueSerializer(val size: Int): ValueSerializer<ShortVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<ShortVectorValue> = Types.ShortVector(this.size)

    override fun fromEntry(entry: ByteIterable): ShortVectorValue = ShortVectorValue(ByteBuffer.wrap(Snappy.uncompress(entry.bytesUnsafe)))

    override fun toEntry(value: ShortVectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}