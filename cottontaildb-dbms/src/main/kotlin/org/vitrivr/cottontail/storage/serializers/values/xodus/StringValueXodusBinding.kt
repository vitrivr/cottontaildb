package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [ComparableBinding] for Xodus based [StringBinding] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class StringValueXodusBinding: XodusBinding<StringValue> {

    override val type = Types.String

    /**
     * [StringValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: StringValueXodusBinding() {
        override val type = Types.String
        override fun entryToValue(entry: ByteIterable): StringValue = StringValue(StringBinding.entryToString(entry))
        override fun valueToEntry(value: StringValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return StringBinding.stringToEntry(value.value)
        }
    }

    /**
     * [StringValueXodusBinding] used for non-nullable values.
     */
    object Nullable: StringValueXodusBinding() {
        /** The special value that is being interpreted as NULL for this [XodusBinding]. */
        private const val NULL_VALUE ="\u0000\u0000"
        private val NULL_VALUE_RAW = StringBinding.stringToEntry(NULL_VALUE)

        override fun entryToValue(entry: ByteIterable): StringValue? {
            if (NULL_VALUE_RAW == entry) return null
            return StringValue(StringBinding.entryToString(entry))
        }
        override fun valueToEntry(value: StringValue?): ByteIterable {
            if (value?.value == NULL_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            if (value == null) return NULL_VALUE_RAW
            return StringBinding.stringToEntry(value.value)
        }
    }
}