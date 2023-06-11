package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedFloatBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [XodusBinding] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class FloatValueXodusBinding: XodusBinding<FloatValue> {
    override val type = Types.Float

    /**
     * [FloatValueXodusBinding] used for nullable values.
     */
    object NonNullable: FloatValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): FloatValue =  FloatValue(SignedFloatBinding.entryToFloat(entry))
        override fun valueToEntry(value: FloatValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return SignedFloatBinding.floatToEntry(value.value)
        }
    }

    /**
     * [FloatValueXodusBinding] used for nullable values.
     */
    object Nullable: FloatValueXodusBinding() {
        private val NULL_VALUE = SignedFloatBinding.floatToEntry(Float.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): FloatValue? {
            if (entry == NULL_VALUE) return null
            return FloatValue(SignedFloatBinding.entryToFloat(entry))
        }

        override fun valueToEntry(value: FloatValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Float.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return SignedFloatBinding.floatToEntry(value.value)
        }
    }
}