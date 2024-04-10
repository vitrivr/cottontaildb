package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object ByteValueValueSerializer: ValueSerializer<ByteValue> {
    override val type = Types.Byte
    override fun write(output: LightOutputStream, value: ByteValue) = ByteBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): ByteValue = ByteValue(ByteBinding.BINDING.readObject(input))
}