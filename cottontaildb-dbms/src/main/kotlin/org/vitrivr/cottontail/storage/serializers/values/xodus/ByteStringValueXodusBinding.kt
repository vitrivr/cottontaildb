package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

sealed class ByteStringValueXodusBinding: XodusBinding<ByteStringValue> {

    override val type = Types.ByteString

    companion object {
        private val NULL_VALUE = ArrayByteIterable(ByteArray(1) { 0 })
    }

    object NonNullable : ByteStringValueXodusBinding() {

        override fun entryToValue(entry: ByteIterable): ByteStringValue {
            return ByteStringValue(Snappy.uncompress(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: ByteStringValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return ArrayByteIterable(Snappy.compress(value.value))
        }

    }

    object Nullable : ByteStringValueXodusBinding() {

        override fun entryToValue(entry: ByteIterable): ByteStringValue? {
            if (NULL_VALUE == entry) return null
            return ByteStringValue(Snappy.uncompress(entry.bytesUnsafe))
        }

        override fun valueToEntry(value: ByteStringValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            return ArrayByteIterable(Snappy.compress(value.value))
        }

    }

}