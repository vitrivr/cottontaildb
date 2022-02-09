package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.ByteIterableUtil
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex32VectorValueXodusBinding(size: Int): XodusBinding<Complex32VectorValue> {
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
            val stream = LightOutputStream(this.type.physicalSize)
            val compressed = Snappy.compress(value.data)
            stream.write(compressed)
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [Complex32VectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): Complex32VectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = FloatBinding.BINDING.objectToEntry(Float.MIN_VALUE)
        }
        override fun entryToValue(entry: ByteIterable): Complex32VectorValue? = if (ByteIterableUtil.compare(NULL_VALUE, entry) == 0) {
            null
        } else {
            Complex32VectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: Complex32VectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            val compressed = Snappy.compress(value.data)
            stream.write(compressed)
            stream.asArrayByteIterable()
        }
    }
}