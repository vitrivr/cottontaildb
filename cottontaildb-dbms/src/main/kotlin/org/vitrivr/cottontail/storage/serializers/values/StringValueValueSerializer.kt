package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue

/**
 * A [ComparableBinding] for Xodus based [StringBinding] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object StringValueValueSerializer: ValueSerializer<StringValue> {
    override val type = Types.String
    override fun fromEntry(entry: ByteIterable): StringValue = StringValue(StringBinding.entryToString(entry))
    override fun toEntry(value: StringValue): ByteIterable = StringBinding.stringToEntry(value.value)
}