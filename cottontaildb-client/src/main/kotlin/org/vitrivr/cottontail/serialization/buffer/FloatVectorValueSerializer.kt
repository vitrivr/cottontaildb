package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueSerializer(size: Int): ValueSerializer<FloatVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<FloatVectorValue> = Types.FloatVector(size)
    override fun fromBuffer(buffer: ByteBuffer) = FloatVectorValue(buffer)
    override fun toBuffer(value: FloatVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) buffer.putFloat(v)
        return buffer.clear()
    }
}