package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object BooleanValueValueSerializer: ValueSerializer<BooleanValue> {
    override val type = Types.Boolean
    override fun write(output: LightOutputStream, value: BooleanValue) = BooleanBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): BooleanValue = BooleanValue(BooleanBinding.BINDING.readObject(input))
}