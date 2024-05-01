package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object UuidValueSerializer: ValueSerializer<UuidValue> {
    override val type = Types.Uuid
    override fun write(output: LightOutputStream, value: UuidValue) {
        LongBinding.BINDING.writeObject(output, value.value.mostSignificantBits)
        LongBinding.BINDING.writeObject(output, value.value.leastSignificantBits)
    }

    override fun read(input: ByteArrayInputStream): UuidValue = UuidValue(LongBinding.BINDING.readObject(input), LongBinding.BINDING.readObject(input))
}