package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [StringBinding] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object StringValueValueSerializer: ValueSerializer<StringValue> {
    override val type = Types.String
    override fun write(output: LightOutputStream, value: StringValue) = StringBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): StringValue = StringValue(StringBinding.BINDING.readObject(input))
}