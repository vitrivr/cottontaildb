package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types

/**
 * A [XodusBinding] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BooleanValueXodusBinding: XodusBinding<BooleanValue> {
    override val type = Types.Boolean

    /**
     * [BooleanValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: BooleanValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): BooleanValue = BooleanValue(BooleanBinding.entryToBoolean(entry))
        override fun valueToEntry(value: BooleanValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return BooleanBinding.booleanToEntry(value.value)
        }
    }

    /**
     * [BooleanValueXodusBinding] used for nullable values.
     */
    object Nullable: BooleanValueXodusBinding() {
        private val NULL_VALUE = ByteBinding.byteToEntry(Byte.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): BooleanValue? {
            if (entry == NULL_VALUE) return null
            return BooleanValue(BooleanBinding.entryToBoolean(entry))
        }
        override fun valueToEntry(value: BooleanValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            return BooleanBinding.booleanToEntry(value.value)
        }
    }
}