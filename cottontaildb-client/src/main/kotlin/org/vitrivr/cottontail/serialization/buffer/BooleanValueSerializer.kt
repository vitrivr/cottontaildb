package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object BooleanValueSerializer: ValueSerializer<BooleanValue> {
    private const val TRUE = 1.toByte()
    private const val FALSE = 0.toByte()
    override val type = Types.Boolean
    override fun fromBuffer(buffer: ByteBuffer) = BooleanValue(buffer.get() == TRUE)
    override fun toBuffer(value: BooleanValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).put(if (value.value) TRUE else FALSE).clear()
}