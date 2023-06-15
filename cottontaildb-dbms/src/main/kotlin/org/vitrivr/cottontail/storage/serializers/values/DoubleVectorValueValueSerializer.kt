package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DoubleVectorValueValueSerializer(size: Int): ValueSerializer<DoubleVectorValue> {

    companion object {
        /** The NULL value for [DoubleVectorValueValueSerializer]s. */
        private val NULL_VALUE = SignedDoubleBinding.BINDING.objectToEntry(Double.MIN_VALUE)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<DoubleVectorValue> = Types.DoubleVector(size)

    /**
     * [DoubleVectorValueValueSerializer] used for non-nullable values.
     */
    class NonNullable(size: Int): DoubleVectorValueValueSerializer(size) {
        override fun fromEntry(entry: ByteIterable): DoubleVectorValue = DoubleVectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
        override fun toEntry(value: DoubleVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [DoubleVectorValueValueSerializer] used for non-nullable values.
     */
    class Nullable(size: Int): DoubleVectorValueValueSerializer(size) {
        override fun fromEntry(entry: ByteIterable): DoubleVectorValue? {
            if (entry == NULL_VALUE) return null
            return DoubleVectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
        }

        override fun toEntry(value: DoubleVectorValue?): ByteIterable {
            if (value == null)return  NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}