package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [ByteStringValue] serialization and deserialization.
 *
 * @author Luca Rossetto
 * @version 1.0.0
 */
data object ByteStringValueSerializer: ValueSerializer<ByteStringValue> {
    override val type = Types.ByteString
    override fun fromBuffer(buffer: ByteBuffer) = ByteStringValue(buffer.array())
    override fun toBuffer(value: ByteStringValue): ByteBuffer = ByteBuffer.wrap(value.value)
}