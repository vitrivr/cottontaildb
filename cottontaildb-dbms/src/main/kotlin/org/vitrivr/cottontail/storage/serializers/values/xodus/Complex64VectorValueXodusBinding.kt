package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [ComparableBinding] for Xodus based [Complex64VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex64VectorValueXodusBinding(size: Int): XodusBinding<Complex64VectorValue> {
    companion object {
        /** The NULL value for [Complex64VectorValueXodusBinding]s. */
        private val NULL_VALUE = DoubleBinding.BINDING.objectToEntry(Double.MIN_VALUE)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<Complex64VectorValue> = Types.Complex64Vector(size)

    /**
     * [Complex64VectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): Complex64VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex64VectorValue = Complex64VectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))

        override fun valueToEntry(value: Complex64VectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [Complex64VectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): Complex64VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex64VectorValue? {
            if (entry == NULL_VALUE) return null
            return Complex64VectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: Complex64VectorValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            val compressed = Snappy.compress(value.data)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }
}