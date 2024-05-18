package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class FloatVectorValueValueSerializer(val size: Int): ValueSerializer<FloatVectorValue> {

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

    override fun write(output: LightOutputStream, value: FloatVectorValue) {
        for (v in value.data) {
            SignedFloatBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = FloatVectorValue(FloatArray(this.size) {
        SignedFloatBinding.BINDING.readObject(input)
    })
}