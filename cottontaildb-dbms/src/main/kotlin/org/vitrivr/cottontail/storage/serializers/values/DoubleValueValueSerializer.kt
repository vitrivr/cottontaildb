package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object DoubleValueValueSerializer: ValueSerializer<DoubleValue> {
    override val type = Types.Double
    override fun write(output: LightOutputStream, value: DoubleValue) = SignedDoubleBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): DoubleValue = DoubleValue(SignedDoubleBinding.BINDING.readObject(input))
}