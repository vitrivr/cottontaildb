package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
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
    companion object {
        /** The NULL value for [IntVectorValueXodusBinding]s. */
        private val NULL_VALUE = IntegerBinding.BINDING.objectToEntry(Int.MIN_VALUE)
    }

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
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [IntVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): IntVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): IntVectorValue? {
            if (NULL_VALUE == entry) return null
            return IntVectorValue(Snappy.uncompressIntArray(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: IntVectorValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}