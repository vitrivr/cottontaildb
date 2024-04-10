package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object Complex32ValueValueSerializer: ValueSerializer<Complex32Value> {
    override val type = Types.Complex32
    override fun write(output: LightOutputStream, value: Complex32Value) {
        SignedFloatBinding.BINDING.writeObject(output, value.real.value)
        SignedFloatBinding.BINDING.writeObject(output, value.real.imaginary)
    }

    override fun read(input: ByteArrayInputStream) = Complex32Value(
        SignedFloatBinding.BINDING.readObject(input),
        SignedFloatBinding.BINDING.readObject(input)
    )
}