package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex64VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class Complex64VectorValueValueSerializer(val size: Int): ValueSerializer<Complex64VectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<Complex64VectorValue> = Types.Complex64Vector(size)

    override fun write(output: LightOutputStream, value: Complex64VectorValue) {
        for (v in value.data) {
            SignedDoubleBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = Complex64VectorValue(DoubleArray(2 * this.size) {
        SignedDoubleBinding.BINDING.readObject(input)
    })
}