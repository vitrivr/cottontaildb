package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BindingUtils
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.DoubleBinding
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

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
        override fun entryToValue(entry: ByteIterable): DoubleValue = DoubleValue(DoubleBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: DoubleValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return DoubleBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [DoubleValueXodusBinding] used for nullable values.
     */
    object Nullable: DoubleValueXodusBinding() {
        private val NULL_VALUE = ByteBinding.BINDING.objectToEntry(Byte.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): DoubleValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                DoubleValue(BindingUtils.readDouble(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: DoubleValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Double.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return DoubleBinding.BINDING.objectToEntry(value.value)
        }
    }
}