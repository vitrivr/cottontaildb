package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class LongValueXodusBinding: XodusBinding<LongValue> {
    override val type = Types.Long

    /**
     * [LongValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: LongValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): LongValue = LongValue(LongBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: LongValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return LongBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [LongValueXodusBinding] used for nullable values.
     */
    object Nullable: LongValueXodusBinding() {
        private val NULL_VALUE = LongBinding.BINDING.objectToEntry(Long.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): LongValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                LongValue(LongBinding.BINDING.readObject(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: LongValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Long.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return LongBinding.BINDING.objectToEntry(value.value)
        }
    }
}