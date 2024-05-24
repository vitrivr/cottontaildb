package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import org.xerial.snappy.Snappy
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] [StringValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
data object StringValueSerializer: ValueSerializer<StringValue> {
    override val type = Types.String
    override fun fromBuffer(buffer: ByteBuffer) = StringValue(Snappy.uncompressString(buffer.array(), 0, buffer.remaining()))
    override fun toBuffer(value: StringValue): ByteBuffer = ByteBuffer.wrap(Snappy.compress(value.value))
}
