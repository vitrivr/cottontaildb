package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object Complex32ValueSerializer: ValueSerializer<Complex32Value> {
    override val type = Types.Complex32
    override fun fromBuffer(buffer: ByteBuffer) = Complex32Value(buffer.getFloat(), buffer.getFloat())
    override fun toBuffer(value: Complex32Value): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putFloat(0, value.data[0]).putFloat(4, value.data[1]).clear()
}