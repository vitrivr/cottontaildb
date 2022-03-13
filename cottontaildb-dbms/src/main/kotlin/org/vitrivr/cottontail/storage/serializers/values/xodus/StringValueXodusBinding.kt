package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.xerial.snappy.Snappy
import java.util.*

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
        override fun entryToValue(entry: ByteIterable): StringValue = StringValue(Snappy.uncompressString(entry.bytesUnsafe))
        override fun valueToEntry(value: StringValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val compressed = Snappy.compress(value.value)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    /**
     * [StringValueXodusBinding] used for non-nullable values.
     */
    object Nullable: StringValueXodusBinding() {
        /** The special value that is being interpreted as NULL for this [XodusBinding]. */
        private const val NULL_VALUE ="\u0000\u0000"
        private val NULL_VALUE_RAW = StringBinding.BINDING.objectToEntry(NULL_VALUE)

        override fun entryToValue(entry: ByteIterable): StringValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE_RAW.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                StringValue(Snappy.uncompressString(entry.bytesUnsafe))
            }
        }
        override fun valueToEntry(value: StringValue?): ByteIterable {
            if (value?.value == NULL_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return if (value == null) {
                NULL_VALUE_RAW
            } else {
                val compressed = Snappy.compress(value.value)
                return ArrayByteIterable(compressed, compressed.size)
            }
        }
    }
}