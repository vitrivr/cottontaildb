package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue

/**
 * A [ValueSerializer] for [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object IntValueValueSerializer: ValueSerializer<IntValue> {
    override val type = Types.Int
    override fun fromEntry(entry: ByteIterable): IntValue =  IntValue(IntegerBinding.entryToInt(entry))
    override fun toEntry(value: IntValue): ByteIterable = IntegerBinding.intToEntry(value.value)
}