package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class LongVectorValueValueSerializer(val size: Int): ValueSerializer<LongVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<LongVectorValue> = Types.LongVector(this.size)

    override fun write(output: LightOutputStream, value: LongVectorValue) {
        for (v in value.data) {
            LongBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = LongVectorValue(LongArray(this.size) {
        LongBinding.BINDING.readObject(input)
    })
}