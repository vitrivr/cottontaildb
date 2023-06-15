package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedFloatBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [ValueSerializer] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class FloatValueValueSerializer: ValueSerializer<FloatValue> {
    override val type = Types.Float

    /**
     * [FloatValueValueSerializer] used for nullable values.
     */
    object NonNullable: FloatValueValueSerializer() {
        override fun fromEntry(entry: ByteIterable): FloatValue =  FloatValue(SignedFloatBinding.entryToFloat(entry))
        override fun toEntry(value: FloatValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return SignedFloatBinding.floatToEntry(value.value)
        }
    }

    /**
     * [FloatValueValueSerializer] used for nullable values.
     */
    object Nullable: FloatValueValueSerializer() {
        private val NULL_VALUE = SignedFloatBinding.floatToEntry(Float.MIN_VALUE)

        override fun fromEntry(entry: ByteIterable): FloatValue? {
            if (entry == NULL_VALUE) return null
            return FloatValue(SignedFloatBinding.entryToFloat(entry))
        }

        override fun toEntry(value: FloatValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Float.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return SignedFloatBinding.floatToEntry(value.value)
        }
    }
}