package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class DoubleVectorValueSerializer(size: Int): ValueSerializer<DoubleVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<DoubleVectorValue> = Types.DoubleVector(size)
    override fun fromBuffer(buffer: ByteBuffer) = DoubleVectorValue(buffer)
    override fun toBuffer(value: DoubleVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) { buffer.putDouble(v) }
        return buffer.clear()
    }
}