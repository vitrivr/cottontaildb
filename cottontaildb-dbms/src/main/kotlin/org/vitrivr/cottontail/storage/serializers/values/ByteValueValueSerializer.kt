package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue

/**
 * A [ValueSerializer] for [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object ByteValueValueSerializer: ValueSerializer<ByteValue> {
    override val type = Types.Byte
    override fun fromEntry(entry: ByteIterable): ByteValue = ByteValue(ByteBinding.entryToByte(entry))
    override fun toEntry(value: ByteValue): ByteIterable = ByteBinding.byteToEntry(value.value)
}