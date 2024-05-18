package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class DoubleVectorValueValueSerializer(val size: Int): ValueSerializer<DoubleVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<DoubleVectorValue> = Types.DoubleVector(size)

    override fun fromBuffer(buffer: ByteBuffer) = DoubleVectorValue(buffer)

    override fun toBuffer(value: DoubleVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.physicalSize)
        for (v in value.data) { buffer.putDouble(v) }
        return buffer.clear()
    }

    override fun write(output: LightOutputStream, value: DoubleVectorValue) {
        for (v in value.data) {
            SignedDoubleBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = DoubleVectorValue(DoubleArray(this.size) {
        SignedDoubleBinding.BINDING.readObject(input)
    })
}