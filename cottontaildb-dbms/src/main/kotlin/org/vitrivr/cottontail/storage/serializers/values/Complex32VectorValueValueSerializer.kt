package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class Complex32VectorValueValueSerializer(val size: Int): ValueSerializer<Complex32VectorValue> {
        init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<Complex32VectorValue> = Types.Complex32Vector(size)
    override fun write(output: LightOutputStream, value: Complex32VectorValue) {
        for (v in value.data) {
            SignedFloatBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = Complex32VectorValue(FloatArray(2 * this.size) {
        SignedFloatBinding.BINDING.readObject(input)
    })
}