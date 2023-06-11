package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [XodusBinding] for [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ShortValueXodusBinding: XodusBinding<ShortValue> {

    override val type = Types.Short

    /**
     * [ShortValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: ShortValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): ShortValue = ShortValue(ShortBinding.entryToShort(entry))
        override fun valueToEntry(value: ShortValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return ShortBinding.shortToEntry(value.value)
        }
    }

    /**
     * [ShortValueXodusBinding] used for nullable values.
     */
    object Nullable: ShortValueXodusBinding() {
        private val NULL_VALUE = ShortBinding.shortToEntry(Short.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): ShortValue? {
            if (entry == NULL_VALUE) return null
            return ShortValue(ShortBinding.entryToShort(entry))
        }

        override fun valueToEntry(value: ShortValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Short.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return ShortBinding.shortToEntry(value.value)
        }
    }
}