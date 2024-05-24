package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object Complex64ValueSerializer: ValueSerializer<Complex64Value> {
    override val type = Types.Complex64
    override fun fromBuffer(buffer: ByteBuffer) = Complex64Value(buffer.getDouble(), buffer.getDouble())
    override fun toBuffer(value: Complex64Value): ByteBuffer = ByteBuffer.allocate(this.type.physicalSize).putDouble(0, value.data[0]).putDouble(8, value.data[1]).clear()
}