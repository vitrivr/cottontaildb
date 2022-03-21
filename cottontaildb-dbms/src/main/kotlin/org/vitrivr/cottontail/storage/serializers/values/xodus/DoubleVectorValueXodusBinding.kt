package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.DoubleBinding
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [XodusBinding] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DoubleVectorValueXodusBinding(size: Int): XodusBinding<DoubleVectorValue> {

    companion object {
        /** The NULL value for [DoubleVectorValueXodusBinding]s. */
        private val NULL_VALUE = DoubleBinding.BINDING.objectToEntry(Double.MIN_VALUE)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<DoubleVectorValue> = Types.DoubleVector(size)

    /**
     * [DoubleVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): DoubleVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): DoubleVectorValue = DoubleVectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
        override fun valueToEntry(value: DoubleVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [DoubleVectorValueXodusBinding] used for non-nullable values.
     */
    class Nullable(size: Int): DoubleVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): DoubleVectorValue? {
            if (entry == NULL_VALUE) return null
            return DoubleVectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: DoubleVectorValue?): ByteIterable {
            if (value == null)return  NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}