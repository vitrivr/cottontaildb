package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object Complex32ValueValueSerializer: ValueSerializer<Complex32Value> {
    override val type = Types.Complex32
    override fun fromEntry(entry: ByteIterable): Complex32Value {
        val stream = ByteArrayInputStream(entry.bytesUnsafe)
        return Complex32Value(SignedFloatBinding.BINDING.readObject(stream), SignedFloatBinding.BINDING.readObject(stream))
    }

    override fun toEntry(value: Complex32Value): ByteIterable {
        val stream = LightOutputStream(this.type.physicalSize)
        SignedFloatBinding.BINDING.writeObject(stream, value.real.value)
        SignedFloatBinding.BINDING.writeObject(stream, value.real.imaginary)
        return stream.asArrayByteIterable()
    }
}