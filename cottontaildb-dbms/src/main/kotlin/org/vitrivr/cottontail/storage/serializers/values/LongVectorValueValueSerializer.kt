package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class LongVectorValueValueSerializer(val size: Int): ValueSerializer<LongVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<LongVectorValue> = Types.LongVector(this.size)

    override fun fromBuffer(buffer: ByteBuffer) = LongVectorValue(buffer)

    override fun toBuffer(value: LongVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) buffer.putLong(v)
        return buffer.clear()
    }

    override fun write(output: LightOutputStream, value: LongVectorValue) {
        for (v in value.data) {
            LongBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = LongVectorValue(LongArray(this.size) {
        LongBinding.BINDING.readObject(input)
    })
}