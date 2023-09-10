package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue

/**
 * A [ValueSerializer] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object BooleanValueValueSerializer: ValueSerializer<BooleanValue> {
    override val type = Types.Boolean
    override fun fromEntry(entry: ByteIterable): BooleanValue = BooleanValue(BooleanBinding.entryToBoolean(entry))
    override fun toEntry(value: BooleanValue): ByteIterable = BooleanBinding.booleanToEntry(value.value)
}