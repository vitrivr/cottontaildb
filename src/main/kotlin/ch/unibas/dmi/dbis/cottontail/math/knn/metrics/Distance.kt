package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import java.lang.Math.toRadians
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
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override val operations: Int = 2
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double =
            StrictMath.sqrt(L2SQUARED(a, b, weights))

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double =
            StrictMath.sqrt(L2SQUARED(a, b, weights))

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double =
            StrictMath.sqrt(L2SQUARED(a, b, weights))

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double =
            StrictMath.sqrt(L2SQUARED(a, b, weights))

        override fun invoke(a: FloatArray, b: FloatArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
        override fun invoke(a: DoubleArray, b: DoubleArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
        override fun invoke(a: LongArray, b: LongArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
        override fun invoke(a: IntArray, b: IntArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
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
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
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
            val div = StrictMath.sqrt(c) * StrictMath.sqrt(d)

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
            val div = StrictMath.sqrt(c) * StrictMath.sqrt(d)

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
            val div = StrictMath.sqrt(c) * StrictMath.sqrt(d)

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
            val div = StrictMath.sqrt(c) * StrictMath.sqrt(d)

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
            val div = StrictMath.sqrt(c) * StrictMath.sqrt(d)

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
            val div = StrictMath.sqrt(c) * StrictMath.sqrt(d)

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
            val div = StrictMath.sqrt(c.toDouble()) * StrictMath.sqrt(d.toDouble())

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
            val div = StrictMath.sqrt(c.toDouble()) * StrictMath.sqrt(d.toDouble())

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

    },

    /**
     * Haversine distance only applicable for two spehere (=earth) coordinates in degrees
     * Hence the arrays <b>have</b> to be of size two each
     */
    HAVERSINE {

        override val operations: Int = 1 // Single calculation as this is fixed.

        /**
         * A constant for the approx. earth radius in meters
         */
        val EARTH_RADIUS = 6371E3 // In meters

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * Hence `a` and `b` **have** to be of size two each. First element in the array is treated
         * as LATITUDE, second as LONGITUDE.
         *
         * As the haversine distance is defined for two points on a sphere, weights are not supported.
         *
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         * @param weights Is not used and not supported
         *
         * @return The haversine distance of the two points `a` and `b`
         */
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            return invoke(a,b)
        }

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * Hence `a` and `b` **have** to be of size two each. First element in the array is treated
         * as LATITUDE, second as LONGITUDE.
         *
         * As the haversine distance is defined for two points on a sphere, weights are not supported.
         *
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         * @param weights Is not used and not supported
         *
         * @return The haversine distance of the two points `a` and `b`
         */
        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            return invoke(a,b)
        }

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * Hence `a` and `b` **have** to be of size two each. First element in the array is treated
         * as LATITUDE, second as LONGITUDE.
         *
         * As the haversine distance is defined for two points on a sphere, weights are not supported.
         *
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         * @param weights Is not used and not supported
         *
         * @return The haversine distance of the two points `a` and `b`
         */
        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            return invoke(a.map { it.toDouble() }.toDoubleArray(), b.map { it.toDouble() }.toDoubleArray())
        }

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * Hence `a` and `b` **have** to be of size two each. First element in the array is treated
         * as LATITUDE, second as LONGITUDE.
         *
         * As the haversine distance is defined for two points on a sphere, weights are not supported.
         *
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         * @param weights Is not used and not supported
         *
         * @return The haversine distance of the two points `a` and `b`
         */
        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            return invoke(a.map{it.toFloat()}.toFloatArray(), b.map{it.toFloat()}.toFloatArray())
        }

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * Hence `a` and `b` **have** to be of size two each. First element in the array is treated
         * as LATITUDE, second as LONGITUDE.
         *
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         *
         * @return The haversine distance of the two points
         */
        override fun invoke(a: FloatArray, b: FloatArray): Double {
            check(a, "a")
            check(b, "b")
            // x[0] = latitude, x[1] = longitude, each in degrees

            var phi1 = a[0].toRadians()
            var phi2 = b[0].toRadians()

            var deltaPhi = (b[0] - a[0]).toRadians()
            var deltaLambda = (b[1] - a[1]).toRadians()

            val c = sin(deltaPhi / 2f) * sin(deltaPhi / 2f) + cos(phi1) * cos(phi2) * sin(deltaLambda / 2f) * sin(
                deltaLambda / 2f
            )
            val d = 2f * atan2(sqrt(c), sqrt(1 - c))

            return EARTH_RADIUS * d
        }

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         * Hence `a` and `b` **have** to be of size two each. First element in the array is treated
         * as LATITUDE, second as LONGITUDE.
         *
         * @param a Start coordinate, where `a[0]` is the LATITUDE, `a[1]` is the LONGITUDE, each in degrees
         * @param b End coordinate, where `b[0]` is the LATITUDE, `b[1]` is the LONGITUDE, each in degrees
         *
         * @return The haversine distance of the two points
         */
        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            check(a, "a")
            check(b, "b")

            // x[0] = latitude, x[1] = longitude, each in degrees

            val phi1 = a[0].toRadians()
            val phi2 = b[0].toRadians()

            val deltaPhi = (b[0] - a[0]).toRadians()
            val deltaLambda = (b[1] - a[1]).toRadians()

            val c = sin(deltaPhi / 2.0) * sin(deltaPhi / 2.0) + cos(phi1) * cos(phi2) * sin(deltaLambda / 2.0) * sin(
                deltaLambda / 2.0
            )
            val d = 2.0 * atan2(sqrt(c), sqrt(1 - c))

            return EARTH_RADIUS.toDouble() * d
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            return invoke(a.map { it.toDouble() }.toDoubleArray(), b.map { it.toDouble() }.toDoubleArray())
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            return invoke(a.map{it.toFloat()}.toFloatArray(), b.map{it.toFloat()}.toFloatArray())
        }

        /**
         * Converts this float in degrees to radians
         * This is an inexact conversion as [PI] is converted to float using [Double.toFloat]
         */
        fun Float.toRadians(): Float {
            return this * PI.toFloat() / 180f
        }

        /**
         * Converts this double in degrees to radians
         */
        fun Double.toRadians(): Double {
            return this * PI / 180.0
        }

        /**
         * Throws an exception, if the given array is not of size two.
         * @param arr The array to check
         */
        private fun check(arr: DoubleArray, label:String = "array"){
            if(arr.size != 2){ // TODO could also be < 2, but then arr[*>=2] would be ignored
                throw ArrayConfigurationNotSupportedException("Haversine distance is only defined for array size of two (2). But $label's size is ${arr.size}")
            }
        }

        /**
         * Throws an exception, if the given array is not of size two.
         * @param arr The array to check
         */
        private fun check(arr: FloatArray, label:String = "array"){
            if(arr.size != 2){ // TODO could also be < 2, but then arr[*>=2] would be ignored
                throw ArrayConfigurationNotSupportedException("Haversine distance is only defined for array size of two (2). But $label's size is ${arr.size}")
            }
        }
    }
}
