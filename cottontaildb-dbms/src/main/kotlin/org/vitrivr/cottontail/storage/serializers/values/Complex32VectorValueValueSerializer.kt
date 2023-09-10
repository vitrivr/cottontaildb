package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.xerial.snappy.Snappy

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Complex32VectorValueValueSerializer(size: Int): ValueSerializer<Complex32VectorValue> {
        init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<Complex32VectorValue> = Types.Complex32Vector(size)
    override fun fromEntry(entry: ByteIterable): Complex32VectorValue = Complex32VectorValue(Snappy.uncompressFloatArray(entry.bytesUnsafe))
    override fun toEntry(value: Complex32VectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}