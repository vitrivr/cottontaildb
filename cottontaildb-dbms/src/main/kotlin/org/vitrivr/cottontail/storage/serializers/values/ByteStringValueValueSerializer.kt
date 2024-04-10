package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [ByteStringValue] serialization and deserialization.
 *
 * @author Luca Rossetto
 * @version 3.0.0
 */
object ByteStringValueValueSerializer: ValueSerializer<ByteStringValue> {
    override val type = Types.ByteString
    override fun read(input: ByteArrayInputStream): ByteStringValue = ByteStringValue(input.readAllBytes())
    override fun write(output: LightOutputStream, value: ByteStringValue) {
       output.write(value.value)
    }
}