package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.xerial.snappy.Snappy

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex32VectorValueXodusBinding(size: Int): XodusBinding<Complex32VectorValue> {
    companion object {
        /** The NULL value for [Complex32VectorValueXodusBinding]s. */
        private val NULL_VALUE = FloatBinding.BINDING.objectToEntry(Float.MIN_VALUE)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<Complex32VectorValue> = Types.Complex32Vector(size)

    /**
     * [Complex32VectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): Complex32VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex32VectorValue = Complex32VectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
        override fun valueToEntry(value: Complex32VectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [Complex32VectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): Complex32VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex32VectorValue? {
            if (entry == NULL_VALUE) return null
            return Complex32VectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: Complex32VectorValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}