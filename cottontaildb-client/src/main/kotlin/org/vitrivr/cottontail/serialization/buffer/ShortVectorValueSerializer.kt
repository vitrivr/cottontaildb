package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [ShortVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ShortVectorValueSerializer(size: Int): ValueSerializer<ShortVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<ShortVectorValue> = Types.ShortVector(size)
    override fun fromBuffer(buffer: ByteBuffer): ShortVectorValue = ShortVectorValue(buffer)
    override fun toBuffer(value: ShortVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) {
            buffer.putShort(v)
        }
        return buffer.clear()
    }
}