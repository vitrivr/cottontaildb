package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.xerial.snappy.Snappy

/**
 * A [ComparableBinding] for Xodus based [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BooleanVectorValueXodusBinding(size: Int): XodusBinding<BooleanVectorValue> {

    companion object {

        /** The NULL value for [BooleanVectorValueXodusBinding]s. */
        private val NULL_VALUE = ByteBinding.BINDING.objectToEntry(Byte.MIN_VALUE)

        /**
         * Initialises a [LongArray] for the given number of booleans to store.
         *
         * @param logicalSize The logical size of the [BooleanVectorValue].
         * @return The corresponding [LongArray].
         */
        protected fun initWordArrayForSize(logicalSize: Int) = LongArray(wordIndex(logicalSize - 1) + 1)

        /**
         * Converts the given [bitIndex] into a word index.
         *
         * @param [bitIndex] The bit index to convert.
         * @return Corresponding word index.
         */
        protected fun wordIndex(bitIndex: Int): Int = bitIndex.shr(6)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    /** The [Types.BooleanVector] handled by this [BooleanValueXodusBinding]. */
    override val type: Types<BooleanVectorValue> = Types.BooleanVector(size)

    /**
     * Converts the given [ByteIterable] into a [BooleanVectorValue].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The corresponding [BooleanVectorValue]
     */
    protected fun internalEntryToValue(entry: ByteIterable): BooleanVectorValue {
        val wordArray = Snappy.uncompressLongArray(entry.bytesUnsafe)
        return BooleanVectorValue(BooleanArray(this.type.logicalSize) {
            (wordArray[wordIndex(it)] and (1L shl it)) != 0L
        })
    }

    /**
     * Serializes the given [BooleanVectorValue] and returns a [ByteIterable]. [BooleanVectorValue]
     * are encoded into long arrays for more efficient storage.
     *
     * @param value The [BooleanVectorValue] to serialize.
     * @return The corresponding [ByteIterable]
     */
    protected fun internalValueToEntry(value: BooleanVectorValue): ByteIterable {
        val wordArray = initWordArrayForSize(this.type.logicalSize)
        for ((i, v) in value.data.withIndex()) {
            val wordIndex = wordIndex(i)
            if (v) {
                wordArray[wordIndex] = wordArray[wordIndex] or (1L shl i)
            } else {
                wordArray[wordIndex] = wordArray[wordIndex] and (1L shl i).inv()
            }
        }
        val compressed = Snappy.compress(wordArray)
        return ArrayByteIterable(compressed, compressed.size)
    }

    /**
     * [BooleanVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): BooleanVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): BooleanVectorValue = this.internalEntryToValue(entry)
        override fun valueToEntry(value: BooleanVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return internalValueToEntry(value)
        }
    }

    /**
     * [BooleanVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): BooleanVectorValueXodusBinding(size)  {
        override fun entryToValue(entry: ByteIterable): BooleanVectorValue? {
            if (NULL_VALUE == entry) return null
            return internalEntryToValue(entry)
        }
        override fun valueToEntry(value: BooleanVectorValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            return this.internalValueToEntry(value)
        }
    }
}