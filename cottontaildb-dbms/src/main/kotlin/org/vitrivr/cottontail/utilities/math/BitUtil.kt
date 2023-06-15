package org.vitrivr.cottontail.utilities.math

import kotlin.experimental.and
import kotlin.experimental.or

object BitUtil {


    /**
     *
     */
    fun nextPowerOfTwo(value: Int): Int = Integer.highestOneBit(value).let {
        if (value == it) {
            nextPowerOfTwo(value + 1)
        } else {
            it shl 1
        }
    }

    /**
     * Checks if the k-th bit is set in the provided [Int]
     *
     * @param k The bit to check.
     * @return True if bit is set, false otherwise.
     */
    fun Int.setBit(k: Int): Int {
        require(k < Int.SIZE_BITS) { "Value $k is out of bounds of type Int." }
        return ((1 shl k) or this)
    }

    /**
     * Checks if the k-th bit is set in the provided [Int]
     *
     * @param k The bit to check.
     * @return True if bit is set, false otherwise.
     */
    fun Int.isBitSet(k: Int): Boolean {
        require(k < Int.SIZE_BITS) { "Value $k is out of bounds of type Int." }
        return (this and (1 shl k)) != 0
    }

    /**
     * Checks if the k-th bit is set in the provided [Long]
     *
     * @param k The bit to set.
     * @return The new [Long] value.
     */
    fun Long.setBit(k: Int): Long {
        require(k < Long.SIZE_BITS) { "Value $k is out of bounds of type Long." }
        return ((1L shl k) or this)
    }


    /**
     * Checks if the k-th bit is set in the provided [Long]
     *
     * @param k The bit to check.
     * @return The new [Long] value.
     */
    fun Long.isBitSet(k: Int): Boolean {
        require(k < Long.SIZE_BITS) { "Value $k is out of bounds of type Long." }
        return (this and (1L shl k)) != 0L
    }
}