package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * A [ValueSerializer] [StringValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
data object StringValueSerializer: ValueSerializer<StringValue> {
    override val type = Types.String
    override fun fromBuffer(buffer: ByteBuffer) = StringValue(StandardCharsets.UTF_8.decode(buffer).toString())
    override fun toBuffer(value: StringValue): ByteBuffer = StandardCharsets.UTF_8.encode(value.value)
}
