package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object UuidValueSerializer: ValueSerializer<UuidValue> {
    override val type = Types.Uuid
    override fun fromEntry(entry: ByteIterable): UuidValue {
        val stream = ByteArrayInputStream(entry.bytesUnsafe)
        return UuidValue(LongBinding.BINDING.readObject(stream), LongBinding.BINDING.readObject(stream))
    }

    override fun toEntry(value: UuidValue): ByteIterable {
        val stream = LightOutputStream(this.type.physicalSize)
        LongBinding.BINDING.writeObject(stream, value.value.leastSignificantBits)
        LongBinding.BINDING.writeObject(stream, value.value.mostSignificantBits)
        return stream.asArrayByteIterable()
    }
}