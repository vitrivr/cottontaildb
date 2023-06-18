package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import org.vitrivr.cottontail.utilities.math.BitUtil.unsetBit
import java.nio.ByteBuffer

/**
 * A [Tablet] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanTablet(override val size: Int, direct: Boolean): Tablet<BooleanValue> {

    /** The [Types] of a [BooleanTablet] is always [Types.Boolean]. */
    override val type: Types<BooleanValue> = Types.Boolean

    /** The raw [ByteBuffer] backing this [BooleanTablet]. */
    override val buffer: ByteBuffer = if (direct) {
        ByteBuffer.allocateDirect(2 * (this.size shr 3))
    } else {
        ByteBuffer.allocate(2 * (this.size shr 3))
    }

    override fun isNull(index: Int): Boolean {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES
        return !this.buffer.getInt(wordIndex).isBitSet(bitPosition)
    }

    override fun get(index: Int): BooleanValue? {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES

        /* Null check is performed within method for efficiency reason. */
        return if (this.buffer.getInt(wordIndex).isBitSet(bitPosition)) {
            BooleanValue(this.buffer.getInt((this.size shr 3) + wordIndex).isBitSet(bitPosition))
        } else {
            null
        }
    }

    override fun set(index: Int, value: BooleanValue?) {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES

        when(value) {
            null ->  this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).unsetBit(bitPosition))
            BooleanValue.TRUE -> {
                this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).setBit(bitPosition))
                this.buffer.putInt((this.size shr 3) + wordIndex, this.buffer.getInt((this.size shr 3) +wordIndex).setBit(bitPosition))
            }
            BooleanValue.FALSE -> {
                this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).setBit(bitPosition))
                this.buffer.putInt((this.size shr 3) + wordIndex, this.buffer.getInt((this.size shr 3) + wordIndex).unsetBit(bitPosition))
            }

        }
    }
}