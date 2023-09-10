package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object Complex64ValueValueSerializer: ValueSerializer<Complex64Value> {
    override val type = Types.Complex64
    override fun fromEntry(entry: ByteIterable): Complex64Value {
        val stream = ByteArrayInputStream(entry.bytesUnsafe)
        return Complex64Value(SignedDoubleBinding.BINDING.readObject(stream),SignedDoubleBinding.BINDING.readObject(stream))
    }

    override fun toEntry(value: Complex64Value): ByteIterable {
        val stream = LightOutputStream(this.type.physicalSize)
        SignedDoubleBinding.BINDING.writeObject(stream, value.real.value)
        SignedDoubleBinding.BINDING.writeObject(stream, value.real.imaginary)
        return stream.asArrayByteIterable()
    }
}