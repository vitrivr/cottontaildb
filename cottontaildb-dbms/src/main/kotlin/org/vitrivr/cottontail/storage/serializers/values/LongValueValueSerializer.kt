package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [ValueSerializer] for [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class LongValueValueSerializer: ValueSerializer<LongValue> {
    override val type = Types.Long

    /**
     * [LongValueValueSerializer] used for non-nullable values.
     */
    object NonNullable: LongValueValueSerializer() {
        override fun fromEntry(entry: ByteIterable): LongValue = LongValue(LongBinding.entryToLong(entry))
        override fun toEntry(value: LongValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return LongBinding.longToEntry(value.value)
        }
    }

    /**
     * [LongValueValueSerializer] used for nullable values.
     */
    object Nullable: LongValueValueSerializer() {
        private val NULL_VALUE = LongBinding.longToEntry(Long.MIN_VALUE)
        override fun fromEntry(entry: ByteIterable): LongValue? {
            if (entry == NULL_VALUE) return null
            return LongValue(LongBinding.entryToLong(entry))
        }

        override fun toEntry(value: LongValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Long.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return LongBinding.longToEntry(value.value)
        }
    }
}