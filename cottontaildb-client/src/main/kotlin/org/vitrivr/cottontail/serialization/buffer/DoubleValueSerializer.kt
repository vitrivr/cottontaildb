package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object DoubleValueSerializer: ValueSerializer<DoubleValue> {
    override val type = Types.Double
    override fun fromBuffer(buffer: ByteBuffer) = DoubleValue(buffer.double)
    override fun toBuffer(value: DoubleValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putDouble(0, value.value)
}