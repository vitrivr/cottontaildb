package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object LongValueValueSerializer: ValueSerializer<LongValue> {
    override val type = Types.Long
    override fun write(output: LightOutputStream, value: LongValue) = LongBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): LongValue = LongValue(LongBinding.BINDING.readObject(input))
}