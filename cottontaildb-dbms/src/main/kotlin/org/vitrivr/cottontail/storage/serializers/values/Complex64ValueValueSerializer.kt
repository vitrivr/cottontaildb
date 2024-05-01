package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object Complex64ValueValueSerializer: ValueSerializer<Complex64Value> {
    override val type = Types.Complex64
    override fun write(output: LightOutputStream, value: Complex64Value) {
        SignedDoubleBinding.BINDING.writeObject(output, value.real.value)
        SignedDoubleBinding.BINDING.writeObject(output, value.real.imaginary)
    }

    override fun read(input: ByteArrayInputStream) = Complex64Value(SignedDoubleBinding.BINDING.readObject(input),SignedDoubleBinding.BINDING.readObject(input))
}