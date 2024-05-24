package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object FloatValueSerializer: ValueSerializer<FloatValue> {
    override val type = Types.Float
    override fun fromBuffer(buffer: ByteBuffer) = FloatValue(buffer.float)
    override fun toBuffer(value: FloatValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putFloat(0, value.value)
}