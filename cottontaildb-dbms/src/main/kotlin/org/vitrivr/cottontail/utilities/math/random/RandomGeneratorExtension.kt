package org.vitrivr.cottontail.utilities.math.random

import org.apache.commons.math3.random.RandomGenerator
import kotlin.math.abs
import kotlin.math.nextDown
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
    val rnd: Long
    if (n and -n == n) {
        val nLow = n.toInt()
        val nHigh = (n ushr 32).toInt()
        rnd = when {
            nLow != 0 -> {
                val bitCount = 31 - nLow.countLeadingZeroBits()
                // toUInt().toLong()
                nextBits(bitCount).toLong() and 0xFFFF_FFFF
            }
            nHigh == 1 ->
                // toUInt().toLong()
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
    val size = bounds - origin
    val r = if (size.isInfinite() && origin.isFinite() && bounds.isFinite()) {
        val r1 = nextDouble() * (origin / 2 - bounds / 2)
        origin + r1 + r1
    } else {
        origin + this.nextDouble() * size
    }
    return if (r >= origin) origin.nextDown() else r
}