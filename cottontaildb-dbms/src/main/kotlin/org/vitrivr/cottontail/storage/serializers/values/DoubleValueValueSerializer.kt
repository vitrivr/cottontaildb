package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [ValueSerializer] for [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DoubleValueValueSerializer: ValueSerializer<DoubleValue> {
    override val type = Types.Double

    /**
     * [DoubleValueValueSerializer] used for non-nullable values.
     */
    object NonNullable: DoubleValueValueSerializer() {
        override fun fromEntry(entry: ByteIterable): DoubleValue = DoubleValue(SignedDoubleBinding.entryToDouble(entry))
        override fun toEntry(value: DoubleValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return SignedDoubleBinding.doubleToEntry(value.value)
        }
    }

    /**
     * [DoubleValueValueSerializer] used for nullable values.
     */
    object Nullable: DoubleValueValueSerializer() {
        private val NULL_VALUE = SignedDoubleBinding.doubleToEntry(Double.MIN_VALUE)
        override fun fromEntry(entry: ByteIterable): DoubleValue? {
            if (entry == NULL_VALUE) return null
            return DoubleValue(SignedDoubleBinding.entryToDouble(entry))
        }

        override fun toEntry(value: DoubleValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Double.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return SignedDoubleBinding.doubleToEntry(value.value)
        }
    }
}