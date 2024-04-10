package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object FloatValueValueSerializer: ValueSerializer<FloatValue> {
    override val type = Types.Float
    override fun write(output: LightOutputStream, value: FloatValue) = SignedFloatBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): FloatValue = FloatValue(SignedFloatBinding.BINDING.readObject(input))
}