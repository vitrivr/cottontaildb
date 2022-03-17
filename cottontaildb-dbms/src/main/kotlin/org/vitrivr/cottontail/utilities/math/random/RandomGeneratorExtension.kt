package org.vitrivr.cottontail.utilities.math.random

import org.apache.commons.math3.random.RandomGenerator
import kotlin.math.abs
import kotlin.random.Random.Default.nextBits


/**
 * Returns a pseudorandom, uniformly distributed [Int] value between [origin] (inclusive) and the specified [bounds] (exclusive).
 *
 * @param origin The origin on the random number to be returned.
 * @param bounds The bound on the random number to be returned. Must be greater than [origin]
 * @return  A pseudorandom, uniformly distributed int value between [origin] (inclusive) and [bounds] (exclusive).
 */
fun RandomGenerator.nextInt(origin: Int, bounds: Int): Int {
    require(bounds > origin) { "Bounds must be greater than origin." }
    return origin + this.nextInt(abs(bounds - origin))
}

/**
 * Returns a pseudorandom, uniformly distributed [Long] value between 0 (inclusive) and the specified [bounds] (exclusive).
 *
 * @param bounds The bound on the random number to be returned. Must be positive.
 * @return  A pseudorandom, uniformly distributed int value between 0 (inclusive) and n (exclusive).
 */
fun RandomGenerator.nextLong(bounds: Long): Long = nextLong(0L, bounds)

/**
 * Returns a pseudorandom, uniformly distributed [Long] value between [origin] (inclusive) and the specified [bounds] (exclusive).
 *
 * @param origin The origin on the random number to be returned.
 * @param bounds The bound on the random number to be returned. Must be greater than [origin]
 * @return  A pseudorandom, uniformly distributed int value between [origin] (inclusive) and [bounds] (exclusive).
 */
fun RandomGenerator.nextLong(origin: Long, bounds: Long): Long {
    require(bounds > origin) { "Bounds must be greater than origin." }
    val n = bounds - origin
    var rnd: Long = 0L
    if (n > 0) {
        if (n and -n == n) {
            val nLow = n.toInt()
            val nHigh = (n ushr 32).toInt()
            rnd = when {
                nLow != 0 -> {
                    val bitCount = 31 - nLow.countLeadingZeroBits()
                    nextBits(bitCount).toLong() and 0xFFFF_FFFF
                }
                nHigh == 1 ->
                    nextInt().toLong() and 0xFFFF_FFFF
                else -> {
                    val bitCount = 31 - nHigh.countLeadingZeroBits()
                    nextBits(bitCount).toLong().shl(32) + (nextInt().toLong() and 0xFFFF_FFFF)
                }
            }
        } else {
            var v: Long
            do {
                val bits = nextLong().ushr(1)
                v = bits % n
            } while (bits - v + (n - 1) < 0)
            rnd = v
        }
    } else {
        while (true) {
            rnd = nextLong()
            if (rnd in origin until bounds) return rnd
        }
    }
    return origin + rnd
}

/**
 * Returns a pseudorandom, uniformly distributed [Double] value between 0.0 (inclusive) and the specified [bounds] (exclusive).
 *
 * @param bounds The bound on the random number to be returned. Must be positive.
 * @return  A pseudorandom, uniformly distributed int value between 0 (inclusive) and n (exclusive).
 */
fun RandomGenerator.nextDouble(bounds: Double): Double = nextDouble(0.0, bounds)

/**
 * Returns a pseudorandom, uniformly distributed [Double] value between [origin] (inclusive) and the specified [bounds] (exclusive).
 *
 * @param origin The origin on the random number to be returned.
 * @param bounds The bound on the random number to be returned. Must be greater than [origin]
 * @return  A pseudorandom, uniformly distributed int value between [origin] (inclusive) and [bounds] (exclusive).
 */
fun RandomGenerator.nextDouble(origin: Double, bounds: Double): Double {
    require(bounds > origin) { "Bounds must be greater than origin." }
    var r = (nextLong() ushr 11) *  (1.0 / (1L.shl(53)));
    if (origin < bounds) {
        r = r * (bounds - origin) + origin
        if (r >= bounds) // correct for rounding
            r = java.lang.Double.longBitsToDouble(java.lang.Double.doubleToLongBits(bounds) - 1)
    }
    return r
}