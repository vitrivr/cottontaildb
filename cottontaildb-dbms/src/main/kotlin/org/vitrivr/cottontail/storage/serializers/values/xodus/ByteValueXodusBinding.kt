package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [XodusBinding] for [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ByteValueXodusBinding: XodusBinding<ByteValue> {
    override val type = Types.Byte

    /**
     * [ByteValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: ByteValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): ByteValue = ByteValue(ByteBinding.entryToByte(entry))
        override fun valueToEntry(value: ByteValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return ByteBinding.byteToEntry(value.value)
        }
    }

    /**
     * [ByteValueXodusBinding] used for nullable values.
     */
    object Nullable: ByteValueXodusBinding() {
        private val NULL_VALUE = ByteBinding.byteToEntry(Byte.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): ByteValue? {
            if (entry == NULL_VALUE) return null
            return ByteValue(ByteBinding.entryToByte(entry))
        }

        override fun valueToEntry(value: ByteValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Byte.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return ByteBinding.byteToEntry(value.value)
        }
    }
}