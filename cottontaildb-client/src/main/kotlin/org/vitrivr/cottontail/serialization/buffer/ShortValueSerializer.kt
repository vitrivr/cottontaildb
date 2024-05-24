package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object ShortValueSerializer: ValueSerializer<ShortValue> {
    override val type = Types.Short
    override fun fromBuffer(buffer: ByteBuffer) = ShortValue(buffer.short)
    override fun toBuffer(value: ShortValue): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putShort(0, value.value)
}