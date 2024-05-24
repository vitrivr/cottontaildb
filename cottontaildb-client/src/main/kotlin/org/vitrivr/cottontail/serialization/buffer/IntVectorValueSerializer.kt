package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorValueSerializer(size: Int): ValueSerializer<IntVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<IntVectorValue> = Types.IntVector(size)
    override fun fromBuffer(buffer: ByteBuffer) = IntVectorValue(buffer)
    override fun toBuffer(value: IntVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) buffer.putInt(v)
        return buffer.clear()
    }
}