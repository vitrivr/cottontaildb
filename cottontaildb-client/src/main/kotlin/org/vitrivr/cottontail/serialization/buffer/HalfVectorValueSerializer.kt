package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.HalfVectorValue
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization with 16 bit precision.
 */
class HalfVectorValueSerializer(size: Int) : ValueSerializer<HalfVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<HalfVectorValue> = Types.HalfVector(size)

    override fun fromBuffer(buffer: ByteBuffer): HalfVectorValue {
        TODO("Not yet implemented")
    }

    override fun toBuffer(value: HalfVectorValue): ByteBuffer {
        TODO("Not yet implemented")
    }
}