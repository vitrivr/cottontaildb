package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue

/**
 * A [ValueSerializer] for [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object ShortValueValueSerializer: ValueSerializer<ShortValue> {
    override val type = Types.Short
    override fun fromEntry(entry: ByteIterable): ShortValue = ShortValue(ShortBinding.entryToShort(entry))
    override fun toEntry(value: ShortValue): ByteIterable = ShortBinding.shortToEntry(value.value)
}