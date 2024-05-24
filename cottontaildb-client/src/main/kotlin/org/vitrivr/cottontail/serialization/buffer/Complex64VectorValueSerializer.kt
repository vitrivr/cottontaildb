package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [Complex64VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class Complex64VectorValueSerializer(size: Int): ValueSerializer<Complex64VectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }
    override val type: Types<Complex64VectorValue> = Types.Complex64Vector(size)
    override fun fromBuffer(buffer: ByteBuffer) = Complex64VectorValue(buffer)
    override fun toBuffer(value: Complex64VectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) { buffer.putDouble(v) }
        return buffer.clear()
    }
}