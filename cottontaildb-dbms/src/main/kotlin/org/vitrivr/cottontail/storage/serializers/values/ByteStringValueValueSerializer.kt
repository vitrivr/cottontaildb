package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.xerial.snappy.Snappy

sealed class ByteStringValueValueSerializer: ValueSerializer<ByteStringValue> {

    override val type = Types.ByteString

    companion object {
        private val NULL_VALUE = ArrayByteIterable(ByteArray(1) { 0 })
    }

    object NonNullable : ByteStringValueValueSerializer() {

        override fun fromEntry(entry: ByteIterable): ByteStringValue {
            return ByteStringValue(Snappy.uncompress(entry.bytesUnsafe))
        }

        override fun toEntry(value: ByteStringValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return ArrayByteIterable(Snappy.compress(value.value))
        }

    }

    object Nullable : ByteStringValueValueSerializer() {

        override fun fromEntry(entry: ByteIterable): ByteStringValue? {
            if (NULL_VALUE == entry) return null
            return ByteStringValue(Snappy.uncompress(entry.bytesUnsafe))
        }

        override fun toEntry(value: ByteStringValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            return ArrayByteIterable(Snappy.compress(value.value))
        }

    }

}