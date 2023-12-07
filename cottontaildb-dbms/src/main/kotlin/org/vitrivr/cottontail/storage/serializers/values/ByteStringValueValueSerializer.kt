package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [ByteStringValue] serialization and deserialization.
 *
 * @author Luca Rossetto
 * @version 2.0.0
 */
object ByteStringValueValueSerializer: ValueSerializer<ByteStringValue> {
    override val type = Types.ByteString
    override fun fromEntry(entry: ByteIterable): ByteStringValue = ByteStringValue(Snappy.uncompress(entry.bytesUnsafe))
    override fun toEntry(value: ByteStringValue): ByteIterable = ArrayByteIterable(Snappy.compress(value.value))
}