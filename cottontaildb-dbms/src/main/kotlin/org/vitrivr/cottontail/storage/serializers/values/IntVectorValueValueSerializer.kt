package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class IntVectorValueValueSerializer(val size: Int): ValueSerializer<IntVectorValue> {

    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<IntVectorValue> = Types.IntVector(this.size)

    override fun write(output: LightOutputStream, value: IntVectorValue) {
        for (v in value.data) {
            IntegerBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream) = IntVectorValue(IntArray(this.size) {
        IntegerBinding.BINDING.readObject(input)
    })
}