package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import org.vitrivr.cottontail.utilities.math.BitUtil.unsetBit
import java.nio.ByteBuffer

/**
 * A [Tablet] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorTablet(override val size: Int, logicalSize: Int, direct: Boolean): Tablet<BooleanVectorValue> {
    init {
        require(this.size % Int.SIZE_BITS == 0) { "Tablet size must be a multiple of 32." }
    }

    /** The [Types] of a [BooleanTablet] is always [Types.Boolean]. */
    override val type: Types<BooleanVectorValue> = Types.BooleanVector(logicalSize)

    /** The raw [ByteBuffer] backing this [BooleanTablet]. */
    override val buffer: ByteBuffer = if (direct) {
        ByteBuffer.allocateDirect( this.size.shr(3) + (this.size * logicalSize).shr(3) + Int.SIZE_BYTES)
    } else {
        ByteBuffer.allocate(this.size.shr(3) + (this.size * logicalSize).shr(3) + Int.SIZE_BYTES)
    }

    override fun isNull(index: Int): Boolean {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES
        return !this.buffer.getInt(wordIndex).isBitSet(bitPosition)
    }

    override fun get(index: Int): BooleanVectorValue? {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        if (isNull(index)) return null

        return BooleanVectorValue(BooleanArray(this.type.logicalSize) {
            val bit = this.type.logicalSize * index + it
            val word = this.size.shr(3) + Int.SIZE_BYTES * bit.shr(5)
            val pos = bit % Int.SIZE_BITS
            this.buffer.getInt(word).isBitSet(pos)
        })
    }

    override fun set(index: Int, value: BooleanVectorValue?) {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES

        if (value == null) {
            this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).unsetBit(bitPosition))
        } else {
            this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).setBit(bitPosition))
            value.data.forEachIndexed { i, b ->
                val bit = this.type.logicalSize * index + i
                val word = this.size.shr(3) + Int.SIZE_BYTES * bit.shr(5)
                val pos = bit % Int.SIZE_BITS
                if (b) {
                    this.buffer.putInt(word, this.buffer.getInt(word).setBit(pos))
                } else {
                    this.buffer.putInt(word, this.buffer.getInt(word).unsetBit(pos))
                }
            }
        }
    }
}