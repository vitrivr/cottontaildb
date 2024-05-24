package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object UuidValueSerializer: ValueSerializer<UuidValue> {
    override val type = Types.Uuid
    override fun fromBuffer(buffer: ByteBuffer) = UuidValue(buffer.long, buffer.long)
    override fun toBuffer(value: UuidValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putLong(value.mostSignificantBits).putLong(value.leastSignificantBits)
}