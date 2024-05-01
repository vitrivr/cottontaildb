package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object DateValueValueSerializer: ValueSerializer<DateValue> {
    override val type = Types.Date
    override fun write(output: LightOutputStream, value: DateValue) = LongBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): DateValue = DateValue(LongBinding.BINDING.readObject(input))
}