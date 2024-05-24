package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object IntValueSerializer: ValueSerializer<IntValue> {
    override val type = Types.Int
    override fun fromBuffer(buffer: ByteBuffer): IntValue = IntValue(buffer.int)
    override fun toBuffer(value: IntValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putInt(0, value.value)
}