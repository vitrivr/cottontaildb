package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [ValueSerializer] for [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object DoubleValueValueSerializer: ValueSerializer<DoubleValue> {
    override val type = Types.Double
    override fun fromEntry(entry: ByteIterable): DoubleValue = DoubleValue(SignedDoubleBinding.entryToDouble(entry))
    override fun toEntry(value: DoubleValue): ByteIterable = SignedDoubleBinding.doubleToEntry(value.value)
}