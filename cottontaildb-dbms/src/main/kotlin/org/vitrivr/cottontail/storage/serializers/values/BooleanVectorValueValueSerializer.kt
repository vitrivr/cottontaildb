package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ValueSerializer] for [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class BooleanVectorValueValueSerializer(val size: Int): ValueSerializer<BooleanVectorValue> {

    companion object {
        /**
         * Converts the given [bitIndex] into a word index.
         *
         * @param [bitIndex] The bit index to convert.
         * @return Corresponding word index.
         */
        fun wordIndex(bitIndex: Int): Int = bitIndex.shr(6)
    }

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    /** The [Types.BooleanVector] handled by this [BooleanValueValueSerializer]. */
    override val type: Types<BooleanVectorValue> = Types.BooleanVector(size)

    /** Number of words that are stored for this [BooleanVectorValue]. */
    private val numberOfWords = wordIndex(this.type.logicalSize - 1) + 1

    override fun write(output: LightOutputStream, value: BooleanVectorValue) {
        val wordArray = LongArray(this.numberOfWords)
        for ((i, v) in value.data.withIndex()) {
            val wordIndex = wordIndex(i)
            if (v) {
                wordArray[wordIndex] = wordArray[wordIndex] or (1L shl i)
            } else {
                wordArray[wordIndex] = wordArray[wordIndex] and (1L shl i).inv()
            }
        }
        for (w in wordArray) {
            LongBinding.BINDING.writeObject(output, w)
        }
    }

    override fun read(input: ByteArrayInputStream): BooleanVectorValue {
        val wordArray = LongArray(this.numberOfWords) {
            LongBinding.BINDING.readObject(input)
        }
        return BooleanVectorValue(BooleanArray(this.type.logicalSize) {
            (wordArray[wordIndex(it)] and (1L shl it)) != 0L
        })
    }
}