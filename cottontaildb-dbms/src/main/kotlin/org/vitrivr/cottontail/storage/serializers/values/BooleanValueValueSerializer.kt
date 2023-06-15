package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue

/**
 * A [ValueSerializer] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BooleanValueValueSerializer: org.vitrivr.cottontail.storage.serializers.values.ValueSerializer<BooleanValue> {
    override val type = Types.Boolean

    /**
     * [BooleanValueValueSerializer] used for non-nullable values.
     */
    object NonNullable: org.vitrivr.cottontail.storage.serializers.values.BooleanValueValueSerializer() {
        override fun fromEntry(entry: ByteIterable): BooleanValue = BooleanValue(BooleanBinding.entryToBoolean(entry))
        override fun toEntry(value: BooleanValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return BooleanBinding.booleanToEntry(value.value)
        }
    }

    /**
     * [BooleanValueValueSerializer] used for nullable values.
     */
    object Nullable: org.vitrivr.cottontail.storage.serializers.values.BooleanValueValueSerializer() {
        private val NULL_VALUE = ByteBinding.byteToEntry(Byte.MIN_VALUE)
        override fun fromEntry(entry: ByteIterable): BooleanValue? {
            if (entry == org.vitrivr.cottontail.storage.serializers.values.BooleanValueValueSerializer.Nullable.NULL_VALUE) return null
            return BooleanValue(BooleanBinding.entryToBoolean(entry))
        }
        override fun toEntry(value: BooleanValue?): ByteIterable {
            if (value == null) return org.vitrivr.cottontail.storage.serializers.values.BooleanValueValueSerializer.Nullable.NULL_VALUE
            return BooleanBinding.booleanToEntry(value.value)
        }
    }
}