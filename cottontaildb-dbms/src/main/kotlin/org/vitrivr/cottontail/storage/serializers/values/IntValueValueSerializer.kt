package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object IntValueValueSerializer: ValueSerializer<IntValue> {
    override val type = Types.Int
    override fun write(output: LightOutputStream, value: IntValue) = IntegerBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): IntValue = IntValue(IntegerBinding.BINDING.readObject(input))
}