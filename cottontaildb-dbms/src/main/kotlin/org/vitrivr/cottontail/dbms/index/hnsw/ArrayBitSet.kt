package org.vitrivr.cottontail.dbms.index.hnsw

import java.io.Serializable
import java.util.*

/**
 * Bitset.
 */
class ArrayBitSet : Serializable {
    private val buffer: IntArray

    /**
     * Initializes a new instance of the [ArrayBitSet] class.
     *
     * @param count The number of items in the set.
     */
    constructor(count: Int) {
        buffer = IntArray((count shr 5) + 1)
    }

    /**
     * Initializes a new instance of the [ArrayBitSet] class. and copies the values
     * of another bitset
     * @param count The number of items in the set.
     */
    constructor(other: ArrayBitSet, count: Int) {
        buffer = Arrays.copyOf(other.buffer, (count shr 5) + 1)
    }

    /**
     * {@inheritDoc}
     */
    operator fun contains(id: Int): Boolean {
        val carrier = buffer[id shr 5]
        return 1 shl (id and 31) and carrier != 0
    }

    /**
     * {@inheritDoc}
     */
    fun add(id: Int) {
        val mask = 1 shl (id and 31)
        buffer[id shr 5] = buffer[id shr 5] or mask
    }

    /**
     * {@inheritDoc}
     */
    fun remove(id: Int) {
        val mask = 1 shl (id and 31)
        buffer[id shr 5] = buffer[id shr 5] and mask.inv()
    }

    /**
     * {@inheritDoc}
     */
    fun clear() {
        Arrays.fill(buffer, 0)
    }

    companion object {
        private const val serialVersionUID = 1L
    }
}