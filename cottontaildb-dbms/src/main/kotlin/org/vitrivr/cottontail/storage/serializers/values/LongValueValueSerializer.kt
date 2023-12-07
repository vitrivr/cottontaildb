package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue

/**
 * A [ValueSerializer] for [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object LongValueValueSerializer: ValueSerializer<LongValue> {
    override val type = Types.Long
    override fun fromEntry(entry: ByteIterable): LongValue = LongValue(LongBinding.entryToLong(entry))
    override fun toEntry(value: LongValue): ByteIterable = LongBinding.longToEntry(value.value)
}