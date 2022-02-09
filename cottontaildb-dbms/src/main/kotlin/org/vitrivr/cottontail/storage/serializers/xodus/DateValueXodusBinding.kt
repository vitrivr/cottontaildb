package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DateValueXodusBinding: XodusBinding<DateValue> {
    override val type = Types.Date

    /**
     * [FloatValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: DateValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): DateValue = DateValue(LongBinding.readCompressed(ByteArrayInputStream(entry.bytesUnsafe)))

        override fun valueToEntry(value: DateValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return LongBinding.longToCompressedEntry(value.value)
        }
    }

    /**
     * [FloatValueXodusBinding] used for nullable values.
     */
    object Nullable: DateValueXodusBinding() {
        private val NULL_VALUE = LongBinding.BINDING.objectToEntry(Byte.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): DateValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                DateValue(LongBinding.readCompressed(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: DateValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Long.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return LongBinding.longToCompressedEntry(value.value)
        }
    }
}