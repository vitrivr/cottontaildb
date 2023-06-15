package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [ValueSerializer] for [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DateValueValueSerializer: ValueSerializer<DateValue> {
    override val type = Types.Date

    /**
     * [FloatValueValueSerializer] used for non-nullable values.
     */
    object NonNullable: DateValueValueSerializer() {
        override fun fromEntry(entry: ByteIterable): DateValue = DateValue(LongBinding.entryToLong(entry))

        override fun toEntry(value: DateValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return LongBinding.longToEntry(value.value)
        }
    }

    /**
     * [FloatValueValueSerializer] used for nullable values.
     */
    object Nullable: DateValueValueSerializer() {
        private val NULL_VALUE = LongBinding.longToEntry(Long.MIN_VALUE)
        override fun fromEntry(entry: ByteIterable): DateValue? {
            if (entry == NULL_VALUE) return null
            return DateValue(LongBinding.entryToLong(entry))
        }

        override fun toEntry(value: DateValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Long.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return LongBinding.longToEntry(value.value)
        }
    }
}