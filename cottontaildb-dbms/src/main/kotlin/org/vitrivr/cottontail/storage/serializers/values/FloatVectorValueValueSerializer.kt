package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedFloatBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class FloatVectorValueValueSerializer(size: Int): ValueSerializer<FloatVectorValue> {

    companion object {
        /** The NULL value for [FloatVectorValueValueSerializer]s. */
        private val NULL_VALUE = SignedFloatBinding.floatToEntry(Float.MIN_VALUE)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<FloatVectorValue> = Types.FloatVector(size)

    /**
     * [FloatVectorValueValueSerializer] used for non-nullable values.
     */
    class NonNullable(size: Int): FloatVectorValueValueSerializer(size) {
        override fun fromEntry(entry: ByteIterable): FloatVectorValue = FloatVectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
        override fun toEntry(value: FloatVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [FloatVectorValueValueSerializer] used for nullable values.
     */
    class Nullable(size: Int): FloatVectorValueValueSerializer(size) {
        override fun fromEntry(entry: ByteIterable): FloatVectorValue? {
            if (NULL_VALUE == entry) return null
            return FloatVectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
        }

        override fun toEntry(value: FloatVectorValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}