package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BooleanValueXodusBinding: XodusBinding<BooleanValue>{
    override val type = Types.Boolean

    /**
     * [BooleanValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: BooleanValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): BooleanValue = BooleanValue(BooleanBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: BooleanValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return BooleanBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [BooleanValueXodusBinding] used for nullable values.
     */
    object Nullable: BooleanValueXodusBinding() {
        private val NULL_VALUE = ByteBinding.BINDING.objectToEntry(Byte.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): BooleanValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                BooleanValue(BooleanBinding.BINDING.readObject(ByteArrayInputStream(bytesRead)))
            }
        }
        override fun valueToEntry(value: BooleanValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            return BooleanBinding.BINDING.objectToEntry(value.value)
        }
    }
}