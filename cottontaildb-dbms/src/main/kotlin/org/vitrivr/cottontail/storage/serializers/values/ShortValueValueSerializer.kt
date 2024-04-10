package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object ShortValueValueSerializer: ValueSerializer<ShortValue> {
    override val type = Types.Short
    override fun write(output: LightOutputStream, value: ShortValue) = ShortBinding.BINDING.writeObject(output, value.value)
    override fun read(input: ByteArrayInputStream): ShortValue = ShortValue(ShortBinding.BINDING.readObject(input))
}