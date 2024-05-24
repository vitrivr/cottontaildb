package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorValueSerializer(size: Int): ValueSerializer<Complex32VectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<Complex32VectorValue> = Types.Complex32Vector(size)
    override fun fromBuffer(buffer: ByteBuffer) = Complex32VectorValue(buffer)
    override fun toBuffer(value: Complex32VectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) buffer.putFloat(v)
        return buffer.clear()
    }
}