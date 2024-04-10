package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [ShortVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class ShortVectorValueValueSerializer(val size: Int): ValueSerializer<ShortVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Types<ShortVectorValue> = Types.ShortVector(this.size)
    override fun write(output: LightOutputStream, value: ShortVectorValue) {
        for (v in value.data) {
            ShortBinding.BINDING.writeObject(output, v)
        }
    }

    override fun read(input: ByteArrayInputStream): ShortVectorValue = ShortVectorValue(ShortArray(this.size) {
        ShortBinding.BINDING.readObject(input)
    })
}