package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.xerial.snappy.Snappy

/**
 * A [ComparableBinding] for Xodus based [Complex64VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 20.0
 */
class Complex64VectorValueValueSerializer(size: Int): ValueSerializer<Complex64VectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<Complex64VectorValue> = Types.Complex64Vector(size)
    override fun fromEntry(entry: ByteIterable): Complex64VectorValue = Complex64VectorValue(Snappy.uncompressDoubleArray(entry.bytesUnsafe))
    override fun toEntry(value: Complex64VectorValue): ByteIterable {
        val compressed = Snappy.compress(value.data)
        return ArrayByteIterable(compressed, compressed.size)
    }
}