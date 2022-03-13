package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.ByteIterableUtil
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [XodusBinding] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class FloatVectorValueXodusBinding(size: Int): XodusBinding<FloatVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<FloatVectorValue> = Types.FloatVector(size)

    /**
     * [FloatVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): FloatVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): FloatVectorValue = FloatVectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
        override fun valueToEntry(value: FloatVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [FloatVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): FloatVectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = FloatBinding.BINDING.objectToEntry(Float.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): FloatVectorValue? {
            return if (ByteIterableUtil.compare(entry, NULL_VALUE) == 0) {
                null
            } else {
                return FloatVectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
            }
        }

        override fun valueToEntry(value: FloatVectorValue?): ByteIterable {
            return if (value == null) {
                NULL_VALUE
            } else {
                val compressed = Snappy.compress(value.data)
                ArrayByteIterable(compressed, compressed.size)
            }
        }
    }
}