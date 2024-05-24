package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorValueSerializer(size: Int): ValueSerializer<BooleanVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type = Types.BooleanVector(size)
    override fun fromBuffer(buffer: ByteBuffer) = BooleanVectorValue(BooleanArray(this.type.logicalSize) {
        (buffer.getLong(Types.BooleanVector.wordIndex(it) * Long.SIZE_BYTES) and (1L shl it)) != 0L
    })
    override fun toBuffer(value: BooleanVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.numberOfWords * Long.SIZE_BYTES)
        for ((i, b) in value.data.withIndex()) {
            val wordIndex = Types.BooleanVector.wordIndex(i) * Long.SIZE_BYTES
            if (b) {
                buffer.putLong(wordIndex, buffer.getLong(wordIndex) or (1L shl i))
            } else {
                buffer.putLong(wordIndex, buffer.getLong(wordIndex) and (1L shl i).inv())
            }
        }
        return buffer.clear()
    }
}