package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class IntVectorValueValueSerializer(val size: Int): ValueSerializer<IntVectorValue> {
    companion object {
        /** The NULL value for [IntVectorValueValueSerializer]s. */
        private val NULL_VALUE = IntegerBinding.BINDING.objectToEntry(Int.MIN_VALUE)
    }

    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<IntVectorValue> = Types.IntVector(this.size)

    /**
     * [IntVectorValueValueSerializer] used for non-nullable values.
     */
    class NonNullable(size: Int): IntVectorValueValueSerializer(size) {
        override fun fromEntry(entry: ByteIterable): IntVectorValue = IntVectorValue(Snappy.uncompressIntArray(entry.bytesUnsafe))
        override fun toEntry(value: IntVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [IntVectorValueValueSerializer] used for nullable values.
     */
    class Nullable(size: Int): IntVectorValueValueSerializer(size) {
        override fun fromEntry(entry: ByteIterable): IntVectorValue? {
            if (NULL_VALUE == entry) return null
            return IntVectorValue(Snappy.uncompressIntArray(entry.bytesUnsafe))
        }

        override fun toEntry(value: IntVectorValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}