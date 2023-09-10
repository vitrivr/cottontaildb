package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.xerial.snappy.Snappy

/**
 * A [ValueSerializer] for [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class BooleanVectorValueValueSerializer(size: Int): ValueSerializer<BooleanVectorValue> {

    companion object {
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

    /** The [Types.BooleanVector] handled by this [BooleanValueValueSerializer]. */
    override val type: Types<BooleanVectorValue> = Types.BooleanVector(size)

    /**
     * Converts the given [ByteIterable] into a [BooleanVectorValue].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The corresponding [BooleanVectorValue]
     */
    private fun internalEntryToValue(entry: ByteIterable): BooleanVectorValue {
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
    private fun internalValueToEntry(value: BooleanVectorValue): ByteIterable {
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

    override fun fromEntry(entry: ByteIterable): BooleanVectorValue = this.internalEntryToValue(entry)

    override fun toEntry(value: BooleanVectorValue): ByteIterable = internalValueToEntry(value)
}