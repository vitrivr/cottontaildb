package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class IntVectorValueValueSerializer(val size: Int): ValueSerializer<IntVectorValue> {

    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<IntVectorValue> = Types.IntVector(this.size)
    override fun fromEntry(entry: ByteIterable): IntVectorValue = IntVectorValue(Snappy.uncompressIntArray(entry.bytesUnsafe))
    override fun toEntry(value: IntVectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}