package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.ByteIterableUtil
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [XodusBinding] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class IntVectorValueXodusBinding(val size: Int): XodusBinding<IntVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<IntVectorValue> = Types.IntVector(this.size)

    /**
     * [IntVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): IntVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): IntVectorValue = IntVectorValue(Snappy.uncompressIntArray(entry.bytesUnsafe))
        override fun valueToEntry(value: IntVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            val compressed = Snappy.compress(value.data)
            stream.write(compressed)
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [IntVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): IntVectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = IntegerBinding.BINDING.objectToEntry(Int.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): IntVectorValue? {
            return if (ByteIterableUtil.compare(NULL_VALUE, entry) == 0) {
                null
            } else {
                return IntVectorValue(Snappy.uncompressIntArray(entry.bytesUnsafe))
            }
        }

        override fun valueToEntry(value: IntVectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            val compressed = Snappy.compress(value.data)
            stream.write(compressed)
            stream.asArrayByteIterable()
        }
    }
}