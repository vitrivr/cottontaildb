package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

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
        override fun entryToValue(entry: ByteIterable): IntValue =  IntValue(IntegerBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: IntValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return IntegerBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [IntValueXodusBinding] used for nullable values.
     */
    object Nullable: IntValueXodusBinding() {
        private val NULL_VALUE = IntegerBinding.BINDING.objectToEntry(Int.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): IntValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                IntValue(IntegerBinding.BINDING.readObject(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: IntValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Int.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return IntegerBinding.BINDING.objectToEntry(value.value)
        }
    }
}