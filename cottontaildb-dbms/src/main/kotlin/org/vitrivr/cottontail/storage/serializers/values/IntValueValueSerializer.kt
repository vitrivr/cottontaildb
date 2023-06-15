package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [ValueSerializer] for [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class IntValueValueSerializer: ValueSerializer<IntValue> {
    override val type = Types.Int

    /**
     * [IntValueValueSerializer] used for non-nullable values.
     */
    object NonNullable: IntValueValueSerializer() {
        override fun fromEntry(entry: ByteIterable): IntValue =  IntValue(IntegerBinding.entryToInt(entry))
        override fun toEntry(value: IntValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return IntegerBinding.intToEntry(value.value)
        }
    }

    /**
     * [IntValueValueSerializer] used for nullable values.
     */
    object Nullable: IntValueValueSerializer() {
        private val NULL_VALUE = IntegerBinding.intToEntry(Int.MIN_VALUE)

        override fun fromEntry(entry: ByteIterable): IntValue? {
            if (entry == NULL_VALUE) return null
            return IntValue(IntegerBinding.entryToInt(entry))
        }

        override fun toEntry(value: IntValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Int.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return IntegerBinding.intToEntry(value.value)
        }
    }
}