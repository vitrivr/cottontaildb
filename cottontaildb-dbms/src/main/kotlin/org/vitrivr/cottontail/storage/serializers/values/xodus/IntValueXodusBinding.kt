package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [XodusBinding] for [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class IntValueXodusBinding: XodusBinding<IntValue> {
    override val type = Types.Int

    /**
     * [IntValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: IntValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): IntValue =  IntValue(IntegerBinding.entryToInt(entry))
        override fun valueToEntry(value: IntValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return IntegerBinding.intToEntry(value.value)
        }
    }

    /**
     * [IntValueXodusBinding] used for nullable values.
     */
    object Nullable: IntValueXodusBinding() {
        private val NULL_VALUE = IntegerBinding.intToEntry(Int.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): IntValue? {
            if (entry == NULL_VALUE) return null
            return IntValue(IntegerBinding.entryToInt(entry))
        }

        override fun valueToEntry(value: IntValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Int.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return IntegerBinding.intToEntry(value.value)
        }
    }
}