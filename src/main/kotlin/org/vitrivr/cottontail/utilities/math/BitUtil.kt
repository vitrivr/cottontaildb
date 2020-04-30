package org.vitrivr.cottontail.utilities.math

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

}