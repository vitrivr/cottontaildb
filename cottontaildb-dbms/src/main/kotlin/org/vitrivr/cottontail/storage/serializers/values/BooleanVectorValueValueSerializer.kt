package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [ValueSerializer] for [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class BooleanVectorValueValueSerializer(val size: Int): ValueSerializer<BooleanVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    /** The [Types.BooleanVector] handled by this [BooleanValueValueSerializer]. */
    override val type = Types.BooleanVector(size)

    override fun fromBuffer(buffer: ByteBuffer) = BooleanVectorValue(BooleanArray(this.type.logicalSize) {
        (buffer.getLong(Types.BooleanVector.wordIndex(it) * Long.SIZE_BYTES) and (1L shl it)) != 0L
    })

    override fun toBuffer(value: BooleanVectorValue): ByteBuffer {
        val buffer = ByteBuffer.allocate(this.type.numberOfWords * Long.SIZE_BYTES)
        for ((i, b) in value.data.withIndex()) {
            val wordIndex = Types.BooleanVector.wordIndex(i) * Long.SIZE_BYTES
            if (b) {
                buffer.putLong(wordIndex, buffer.getLong(wordIndex) or (1L shl i))
            } else {
                buffer.putLong(wordIndex, buffer.getLong(wordIndex) and (1L shl i).inv())
            }
        }
        return buffer.clear()
    }

    override fun write(output: LightOutputStream, value: BooleanVectorValue) {
        val wordArray = LongArray(this.type.numberOfWords)
        for ((i, v) in value.data.withIndex()) {
            val wordIndex = Types.BooleanVector.wordIndex(i)
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
        val wordArray = LongArray(this.type.numberOfWords) {
            LongBinding.BINDING.readObject(input)
        }
        return BooleanVectorValue(BooleanArray(this.type.logicalSize) {
            (wordArray[Types.BooleanVector.wordIndex(it)] and (1L shl it)) != 0L
        })
    }
}