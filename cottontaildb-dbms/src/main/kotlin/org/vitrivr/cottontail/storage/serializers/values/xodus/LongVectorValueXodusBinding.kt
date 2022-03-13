package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.ByteIterableUtil
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [XodusBinding] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class LongVectorValueXodusBinding(val size: Int): XodusBinding<LongVectorValue> {

    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<LongVectorValue> = Types.LongVector(this.size)

    /**
     * [LongVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): LongVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): LongVectorValue = LongVectorValue(Snappy.uncompressLongArray(entry.bytesUnsafe))

        override fun valueToEntry(value: LongVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [LongVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): LongVectorValueXodusBinding(size) {

        companion object {
            private val NULL_VALUE = LongBinding.BINDING.objectToEntry(Long.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): LongVectorValue? {
            return if (ByteIterableUtil.compare(NULL_VALUE, entry) == 0) {
                null
            } else {
                return LongVectorValue(Snappy.uncompressLongArray(entry.bytesUnsafe))
            }
        }

        override fun valueToEntry(value: LongVectorValue?): ByteIterable {
            return if (value == null) {
                NULL_VALUE
            } else {
                val compressed = Snappy.compress(value.data)
                ArrayByteIterable(compressed, compressed.size)
            }
        }
    }

}