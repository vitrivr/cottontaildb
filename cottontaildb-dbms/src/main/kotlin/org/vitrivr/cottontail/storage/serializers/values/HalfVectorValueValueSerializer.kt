package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.HalfVectorValue
import org.vitrivr.cottontail.math.Half
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [FloatVectorValue] serialization and deserialization with 16 bit precision.
 */
class HalfVectorValueValueSerializer(val size: Int) : ValueSerializer<HalfVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<HalfVectorValue> = Types.HalfVector(size)

    override fun read(input: ByteArrayInputStream) = HalfVectorValue(FloatArray(this.size) {
        Half(ShortBinding.BINDING.readObject(input).toUShort()).toFloat()
    })

    override fun write(output: LightOutputStream, value: HalfVectorValue) {
        for (v in value.data) {
            SignedFloatBinding.BINDING.writeObject(output, Half(v).toShort())
        }
    }
}