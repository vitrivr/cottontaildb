package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.ByteIterableUtil
import jetbrains.exodus.util.LightOutputStream
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
            val stream = LightOutputStream(this.type.physicalSize)
            val compressed = Snappy.compress(value.data)
            stream.write(compressed)
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [Complex64VectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): Complex64VectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = DoubleBinding.BINDING.objectToEntry(Double.MIN_VALUE)
        }
        override fun entryToValue(entry: ByteIterable): Complex64VectorValue? = if (ByteIterableUtil.compare(NULL_VALUE, entry) == 0) {
            null
        } else {
            Complex64VectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: Complex64VectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            val compressed = Snappy.compress(value.data)
            stream.write(compressed)
            stream.asArrayByteIterable()
        }
    }
}