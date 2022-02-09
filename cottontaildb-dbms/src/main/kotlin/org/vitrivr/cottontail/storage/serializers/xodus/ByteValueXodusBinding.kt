package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

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
        override fun entryToValue(entry: ByteIterable): ByteValue = ByteValue(ByteBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: ByteValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return ByteBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [ByteValueXodusBinding] used for nullable values.
     */
    object Nullable: ByteValueXodusBinding() {
        private val NULL_VALUE = ByteBinding.BINDING.objectToEntry(Byte.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): ByteValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                ByteValue(ByteBinding.BINDING.readObject(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: ByteValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Byte.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return ByteBinding.BINDING.objectToEntry(value.value)
        }
    }
}