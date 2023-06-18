package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import org.vitrivr.cottontail.utilities.math.BitUtil.unsetBit
import java.nio.ByteBuffer

/**
 * An abstract [Tablet] implementation, that can be used for most types. Used to group [Value]s, that are processed or serialized together.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractTablet<T: Value>(final override val size: Int, final override val type: Types<T>, direct: Boolean): Tablet<T> {

    init {
        require(this.size % Int.SIZE_BITS == 0) { "Tablet size must be a multiple of 32." }
    }

    /** The raw [ByteBuffer] backing this [AbstractTablet]. */
    override val buffer: ByteBuffer = if (direct) {
        ByteBuffer.allocateDirect((this.size shr 3) + (this.size * this.type.physicalSize))
    } else {
        ByteBuffer.allocate((this.size shr 3) + (this.size * this.type.physicalSize))
    }

    /**
     * Checks if the value at position [index] is set (or not).
     *
     * @param index The index to check.
     * @return True if index is set, false otherwise.
     */
    override fun isNull(index: Int): Boolean {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES
        return !this.buffer.getInt(wordIndex).isBitSet(bitPosition)
    }

    /**
     * Gets and returns a [Value] held in this [AbstractTablet].
     *
     * @param index The index of the [Value] to return.
     * @return [Value] or null
     */
    override operator fun get(index: Int): T? {
        if (isNull(index)) return null
        return internalGet(index)
    }

    /**
     * Set a [Value] in this [AbstractTablet].
     *
     * @param index The index of the [Value] to set.
     * @param value The new [Value] or null
     */
    override operator fun set(index: Int, value: T?) {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        val bitIndex = index shr 5
        val bitPosition = index % Int.SIZE_BITS
        val wordIndex = bitIndex * Int.SIZE_BYTES
        if (value == null) {
            this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).unsetBit(bitPosition))
            return
        }
        this.buffer.putInt(wordIndex, this.buffer.getInt(wordIndex).setBit(bitPosition))
        this.internalSet(index, value)
    }

    /**
     * Obtains the buffer position for the provided [index]
     *
     * @param index The index to obtain position for.
     * @return The position.
     */
    protected fun indexToPosition(index: Int): Int {
        require(index < this.size) { "Provided index $index is out of bounds for this tablet." }
        return (this.size shr 3) + (index * this.type.physicalSize)
    }

    /**
     * Reads a value of type [T] to the current [buffer] position.
     *
     * @return [T]
     */
    protected abstract fun internalGet(index: Int): T

    /**
     * Writes a value of type [T] to the current [buffer] position.
     *
     * @return [T]
     */
    abstract fun internalSet(index: Int, value: T)
}