package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedFloatBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue

/**
 * A [ValueSerializer] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object FloatValueValueSerializer: ValueSerializer<FloatValue> {
    override val type = Types.Float
    override fun fromEntry(entry: ByteIterable): FloatValue =  FloatValue(SignedFloatBinding.entryToFloat(entry))
    override fun toEntry(value: FloatValue): ByteIterable = SignedFloatBinding.floatToEntry(value.value)
}