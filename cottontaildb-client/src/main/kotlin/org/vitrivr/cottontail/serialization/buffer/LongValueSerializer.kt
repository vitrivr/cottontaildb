package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object LongValueSerializer: ValueSerializer<LongValue> {
    override val type = Types.Long
    override fun fromBuffer(buffer: ByteBuffer): LongValue = LongValue(buffer.long)
    override fun toBuffer(value: LongValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putLong(0, value.value)
}