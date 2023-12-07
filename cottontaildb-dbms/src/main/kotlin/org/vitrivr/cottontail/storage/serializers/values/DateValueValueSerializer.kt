package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue

/**
 * A [ValueSerializer] for [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object DateValueValueSerializer: ValueSerializer<DateValue> {
    override val type = Types.Date
    override fun fromEntry(entry: ByteIterable): DateValue = DateValue(LongBinding.entryToLong(entry))
    override fun toEntry(value: DateValue): ByteIterable = LongBinding.longToEntry(value.value)
}