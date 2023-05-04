package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [XodusBinding] for [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DoubleValueXodusBinding: XodusBinding<DoubleValue> {
    override val type = Types.Double

    /**
     * [DoubleValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: DoubleValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): DoubleValue = DoubleValue(SignedDoubleBinding.entryToDouble(entry))
        override fun valueToEntry(value: DoubleValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return SignedDoubleBinding.doubleToEntry(value.value)
        }
    }

    /**
     * [DoubleValueXodusBinding] used for nullable values.
     */
    object Nullable: DoubleValueXodusBinding() {
        private val NULL_VALUE = SignedDoubleBinding.doubleToEntry(Double.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): DoubleValue? {
            if (entry == NULL_VALUE) return null
            return DoubleValue(SignedDoubleBinding.entryToDouble(entry))
        }

        override fun valueToEntry(value: DoubleValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Double.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return SignedDoubleBinding.doubleToEntry(value.value)
        }
    }
}