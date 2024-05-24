package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object ByteValueSerializer: ValueSerializer<ByteValue> {
    override val type = Types.Byte
    override fun fromBuffer(buffer: ByteBuffer): ByteValue = ByteValue(buffer.get())
    override fun toBuffer(value: ByteValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).put(0, value.value)
}