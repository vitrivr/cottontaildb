package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object DateValueSerializer: ValueSerializer<DateValue> {
    override val type = Types.Date
    override fun fromBuffer(buffer: ByteBuffer) = DateValue(buffer.long)
    override fun toBuffer(value: DateValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putLong(0, value.value)
}