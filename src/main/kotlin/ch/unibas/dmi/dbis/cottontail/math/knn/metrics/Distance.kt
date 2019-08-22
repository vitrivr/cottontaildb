package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.minus
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.plus
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.times
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.toDouble
import java.util.*
import kotlin.math.*

enum class Distance : DistanceFunction {
    /**
     * L1 or Manhattan distance between two vectors. Vectors must be of the same size!
     */
    L1 {
        override val operations: Int = 1

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size()) {
                sum += abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: BitSet, b: BitSet): Double {
            var sum = 0.0
            for (i in 0 until b.size()) {
                sum += when {
                    !a[i] && b[i] -> 1
                    a[i] && !b[i] -> -1
                    else -> 0
                }
            }
            return sum
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override val operations: Int = 2
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: FloatArray, b: FloatArray): Double = sqrt(L2SQUARED(a, b))
        override fun invoke(a: DoubleArray, b: DoubleArray): Double = sqrt(L2SQUARED(a, b))
        override fun invoke(a: LongArray, b: LongArray): Double = sqrt(L2SQUARED(a, b))
        override fun invoke(a: IntArray, b: IntArray): Double = sqrt(L2SQUARED(a, b))
        override fun invoke(a: BitSet, b: BitSet): Double = sqrt(L2SQUARED(a, b))
    },

    /**
     * Squared L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {

        override val operations: Int = 2

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size()) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: BitSet, b: BitSet): Double {
            var sum = 0.0
            for (i in 0 until b.size()) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }
    },

    /**
     * Chi Squared distance between two vectors. Vectors must be of the same size!
     */
    CHISQUARED {

        override val operations: Int = 3

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size()) {
                if (abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: BitSet, b: BitSet): Double {
            var sum = 0.0
            for (i in 0 until b.size()) {
                if (abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }
    },

    COSINE {

        override val operations: Int = 3

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size) {
                dot += a[i] * b[i] * weights[i]
                c += a[i] * a[i] * weights[i]
                d += b[i] * b[i] * weights[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }

        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size) {
                dot += a[i] * b[i] * weights[i]
                c += a[i] * a[i] * weights[i]
                d += b[i] * b[i] * weights[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size) {
                dot += a[i] * b[i] * weights[i]
                c += a[i] * a[i] * weights[i]
                d += b[i] * b[i] * weights[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size) {
                dot += a[i] * b[i] * weights[i]
                c += a[i] * a[i] * weights[i]
                d += b[i] * b[i] * weights[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size()) {
                dot += a[i] * b[i] * weights[i]
                c += a[i] * a[i] * weights[i]
                d += b[i] * b[i] * weights[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size) {
                dot += a[i] * b[i]
                c += a[i] * a[i]
                d += b[i] * b[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0

            for (i in 0 until b.size) {
                dot += a[i] * b[i]
                c += a[i] * a[i]
                d += b[i] * b[i]
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var dot = 0L
            var c = 0L
            var d = 0L

            for (i in 0 until b.size) {
                dot += a[i] * b[i]
                c += a[i] * a[i]
                d += b[i] * b[i]
            }
            val div = sqrt(c.toDouble()) * sqrt(d.toDouble())

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var dot = 0L
            var c = 0L
            var d = 0L

            for (i in 0 until b.size) {
                dot += a[i] * b[i]
                c += a[i] * a[i]
                d += b[i] * b[i]
            }
            val div = sqrt(c.toDouble()) * sqrt(d.toDouble())

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: BitSet, b: BitSet): Double {
            var dot = 0L
            var c = 0L
            var d = 0L

            for (i in 0 until b.size()) {
                dot += a[i] * b[i]
                c += a[i] * a[i]
                d += b[i] * b[i]
            }
            val div = sqrt(c.toDouble()) * sqrt(d.toDouble())

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }
    },

    /**
     * Hamming distance: Makes an element wise comparison of the two arrays and increases the distance by 1, everytime two corresponding elements don't match.
     */
    HAMMING {
        override val operations: Int = 1
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0f else weights[i] }.sum().toDouble()
        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else weights[i] }.sum()
        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0f else weights[i] }.sum().toDouble()
        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0f else weights[i] }.sum().toDouble()
        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double = (0 until b.size()).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0f else weights[i] }.sum().toDouble()
        override fun invoke(a: FloatArray, b: FloatArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0 }.sum()
        override fun invoke(a: DoubleArray, b: DoubleArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0 }.sum()
        override fun invoke(a: LongArray, b: LongArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0 }.sum()
        override fun invoke(a: IntArray, b: IntArray): Double = (0 until b.size).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0}.sum()
        override fun invoke(a: BitSet, b: BitSet): Double = (0 until b.size()).mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0 }.sum()
    },

    /**
     * Haversine distance only applicable for two spherical (= earth) coordinates in degrees. Hence the arrays <b>have</b> to be of size two each
     */
    HAVERSINE {
        
        override val operations: Int = 1 // Single calculation as this is fixed.

        /**
         * A constant for the approx. earth radius in meters
         */
        private val EARTH_RADIUS = 6371E3 // In meters

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double = this.haversine(a[0], a[1], b[0], b[1])
        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: BitSet, b: BitSet, weights: FloatArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: FloatArray, b: FloatArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: DoubleArray, b: DoubleArray): Double = this.haversine(a[0], a[1], b[0], b[1])
        override fun invoke(a: LongArray, b: LongArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: IntArray, b: IntArray): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())
        override fun invoke(a: BitSet, b: BitSet): Double = this.haversine(a[0].toDouble(), a[1].toDouble(), b[0].toDouble(), b[1].toDouble())

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         *
         * @return The haversine distance between the two points
         */
        private fun haversine(a_lat: Double, a_lon: Double, b_lat: Double, b_lon: Double): Double {
            val phi1 = StrictMath.toRadians(a_lat)
            val phi2 = StrictMath.toRadians(b_lat)
            val deltaPhi = StrictMath.toRadians(b_lat - a_lat)
            val deltaLambda = StrictMath.toRadians(b_lon - a_lon)
            val c = sin(deltaPhi / 2.0) * sin(deltaPhi / 2.0) + cos(phi1) * cos(phi2) * sin(deltaLambda / 2.0) * sin(
                    deltaLambda / 2.0
            )
            val d = 2.0 * atan2(sqrt(c), sqrt(1 - c))
            return EARTH_RADIUS * d
        }
    }
}
