package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorValueSerializer(size: Int): ValueSerializer<LongVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<LongVectorValue> = Types.LongVector(size)
    override fun fromBuffer(buffer: ByteBuffer) = LongVectorValue(buffer)
    override fun toBuffer(value: LongVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) buffer.putLong(v)
        return buffer.clear()
    }
}