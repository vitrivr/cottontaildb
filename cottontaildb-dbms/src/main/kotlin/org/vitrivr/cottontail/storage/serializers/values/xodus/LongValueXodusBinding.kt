package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

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
        override fun entryToValue(entry: ByteIterable): LongValue = LongValue(LongBinding.entryToLong(entry))
        override fun valueToEntry(value: LongValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return LongBinding.longToEntry(value.value)
        }
    }

    /**
     * [LongValueXodusBinding] used for nullable values.
     */
    object Nullable: LongValueXodusBinding() {
        private val NULL_VALUE = LongBinding.longToEntry(Long.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): LongValue? {
            if (entry == NULL_VALUE) return null
            return LongValue(LongBinding.entryToLong(entry))
        }

        override fun valueToEntry(value: LongValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Long.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return LongBinding.longToEntry(value.value)
        }
    }
}