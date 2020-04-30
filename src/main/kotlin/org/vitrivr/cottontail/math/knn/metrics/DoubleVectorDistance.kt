package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.VectorValue

import kotlin.math.*

enum class DoubleVectorDistance : VectorizedDistanceFunction<DoubleArray> {
    /**
     * L1 or Manhattan distance between two vectors. Vectors must be of the same size!
     */
    L1 {
        override val operations: Int = 1

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, shape: Shape): Double {
            var sum1 = 0.0
            var sum2 = 0.0
            var sum3 = 0.0
            var sum4 = 0.0
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                sum1 += abs(b.getAsDouble(i * VECTORIZATION) - a.getAsDouble(i * VECTORIZATION))
                sum2 += abs(b.getAsDouble(i * VECTORIZATION + 1) - a.getAsDouble(i * VECTORIZATION + 1))
                sum3 += abs(b.getAsDouble(i * VECTORIZATION + 2) - a.getAsDouble(i * VECTORIZATION + 2))
                sum4 += abs(b.getAsDouble(i * VECTORIZATION + 3) - a.getAsDouble(i * VECTORIZATION + 3))
            }
            for (i in max * VECTORIZATION until b.size) {
                sum4 += abs(b.getAsDouble(i) - a.getAsDouble(i))
            }
            return sum1 + sum2 + sum3 + sum4
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>, shape: Shape): Double {
            var sum1 = 0.0
            var sum2 = 0.0
            var sum3 = 0.0
            var sum4 = 0.0
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                sum1 = Math.fma(abs(b.getAsDouble(i * VECTORIZATION) - a.getAsDouble(i * VECTORIZATION)), weights.getAsDouble(i * VECTORIZATION), sum1)
                sum2 = Math.fma(abs(b.getAsDouble(i * VECTORIZATION + 1) - a.getAsDouble(i * VECTORIZATION + 1)), weights.getAsDouble(i * VECTORIZATION + 1), sum2)
                sum3 = Math.fma(abs(b.getAsDouble(i * VECTORIZATION + 2) - a.getAsDouble(i * VECTORIZATION + 2)), weights.getAsDouble(i * VECTORIZATION + 2), sum3)
                sum4 = Math.fma(abs(b.getAsDouble(i * VECTORIZATION + 3) - a.getAsDouble(i * VECTORIZATION + 3)), weights.getAsDouble(i * VECTORIZATION + 3), sum4)
            }
            for (i in max * VECTORIZATION until b.size) {
                sum4 = Math.fma(abs(b.getAsDouble(i) - a.getAsDouble(i)), weights.getAsDouble(i), sum4)
            }
            return sum1 + sum2 + sum3 + sum4
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double {
            var sum = 0.0
            for (i in b.indices) {
                sum += abs(b.getAsDouble(i) - a.getAsDouble(i)) * weights.getAsDouble(i)
            }
            return sum
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double {
            var sum = 0.0
            for (i in b.indices) {
                sum += abs(b.getAsDouble(i) - a.getAsDouble(i))
            }
            return sum
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override val operations: Int = 2
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, shape: Shape): Double = sqrt(L2SQUARED(a, b, shape))
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>, shape: Shape): Double = sqrt(L2SQUARED(a, b, weights, shape))
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double = sqrt(L2SQUARED(a, b))
    },

    /**
     * Squared L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {
        override val operations: Int = 2

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, shape: Shape): Double {
            var sum1 = 0.0
            var sum2 = 0.0
            var sum3 = 0.0
            var sum4 = 0.0
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                sum1 += (b.getAsDouble(i * VECTORIZATION) - a.getAsDouble(i * VECTORIZATION)).pow(2.0)
                sum2 += (b.getAsDouble(i * VECTORIZATION + 1) - a.getAsDouble(i * VECTORIZATION + 1)).pow(2.0)
                sum3 += (b.getAsDouble(i * VECTORIZATION + 2) - a.getAsDouble(i * VECTORIZATION + 2)).pow(2.0)
                sum4 += (b.getAsDouble(i * VECTORIZATION + 3) - a.getAsDouble(i * VECTORIZATION + 3)).pow(2.0)
            }
            for (i in max * VECTORIZATION until b.size) {
                sum4 += (b.getAsDouble(i) - a.getAsDouble(i)).pow(2.0)
            }
            return sum1 + sum2 + sum3 + sum4
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>, shape: Shape): Double {
            var sum1 = 0.0
            var sum2 = 0.0
            var sum3 = 0.0
            var sum4 = 0.0
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                sum1 = Math.fma(b.getAsDouble(i * VECTORIZATION) - a.getAsDouble(i * VECTORIZATION), (b.getAsDouble(i * VECTORIZATION) - a.getAsDouble(i * VECTORIZATION)) * weights.getAsDouble(i * VECTORIZATION), sum1)
                sum2 = Math.fma(b.getAsDouble(i * VECTORIZATION + 1) - a.getAsDouble(i * VECTORIZATION + 1), (b.getAsDouble(i * VECTORIZATION + 1) - a.getAsDouble(i * VECTORIZATION + 1)) * weights.getAsDouble(i * VECTORIZATION + 1), sum2)
                sum3 = Math.fma(b.getAsDouble(i * VECTORIZATION + 2) - a.getAsDouble(i * VECTORIZATION + 2), (b.getAsDouble(i * VECTORIZATION + 2) - a.getAsDouble(i * VECTORIZATION + 2)) * weights.getAsDouble(i * VECTORIZATION + 2), sum3)
                sum4 = Math.fma(b.getAsDouble(i * VECTORIZATION + 3) - a.getAsDouble(i * VECTORIZATION + 3), (b.getAsDouble(i * VECTORIZATION + 3) - a.getAsDouble(i * VECTORIZATION + 3) * weights.getAsDouble(i * VECTORIZATION + 3)), sum4)
            }
            for (i in max * VECTORIZATION until b.size) {
                sum4 = Math.fma(b.getAsDouble(i) - a.getAsDouble(i), (b.getAsDouble(i) - a.getAsDouble(i)) * weights.getAsDouble(i), sum4)
            }
            return sum1 + sum2 + sum3 + sum4
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double {
            var sum = 0.0
            for (i in b.indices) {
                sum += (b.getAsDouble(i) - a.getAsDouble(i)) * (b.getAsDouble(i) - a.getAsDouble(i)) * weights.getAsDouble(i)
            }
            return sum
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double {
            var sum = 0.0
            for (i in b.indices) {
                sum += (b.getAsDouble(i) - a.getAsDouble(i)) * (b.getAsDouble(i) - a.getAsDouble(i))
            }
            return sum
        }
    },

    /**
     * Chi Squared distance between two vectors. Vectors must be of the same size!
     */
    CHISQUARED {

        override val operations: Int = 3

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double {
            var sum = 0.0
            for (i in b.indices) {
                if (abs(a.getAsDouble(i) + b.getAsDouble(i)) > 1e-6) {
                    sum += ((b.getAsDouble(i) - a.getAsDouble(i)) * (b.getAsDouble(i) - a.getAsDouble(i))) / (b.getAsDouble(i) + a.getAsDouble(i)) * weights.getAsDouble(i)
                }
            }
            return sum
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double {
            var sum = 0.0
            for (i in b.indices) {
                if (abs(a.getAsDouble(i) + b.getAsDouble(i)) > 1e-6) {
                    sum += ((b.getAsDouble(i) - a.getAsDouble(i)) * (b.getAsDouble(i) - a.getAsDouble(i))) / (b.getAsDouble(i) + a.getAsDouble(i))
                }
            }
            return sum
        }
    },

    COSINE {
        override val operations: Int = 3

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>, shape: Shape): Double {
            var dot1 = 0.0
            var dot2 = 0.0
            var dot3 = 0.0
            var dot4 = 0.0

            var c1 = 0.0
            var c2 = 0.0
            var c3 = 0.0
            var c4 = 0.0

            var d1 = 0.0
            var d2 = 0.0
            var d3 = 0.0
            var d4 = 0.0

            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                dot1 = Math.fma(a.getAsDouble(i * VECTORIZATION), b.getAsDouble(i * VECTORIZATION) * weights.getAsDouble(i * VECTORIZATION), dot1)
                dot2 = Math.fma(a.getAsDouble(i * VECTORIZATION + 1), b.getAsDouble(i * VECTORIZATION + 1) * weights.getAsDouble(i * VECTORIZATION + 1), dot2)
                dot3 = Math.fma(a.getAsDouble(i * VECTORIZATION + 2), b.getAsDouble(i * VECTORIZATION + 2) * weights.getAsDouble(i * VECTORIZATION + 2), dot3)
                dot4 = Math.fma(a.getAsDouble(i * VECTORIZATION + 3), b.getAsDouble(i * VECTORIZATION + 3) * weights.getAsDouble(i * VECTORIZATION + 3), dot4)

                c1 = Math.fma(a.getAsDouble(i * VECTORIZATION), a.getAsDouble(i * VECTORIZATION) * weights.getAsDouble(i * VECTORIZATION), c1)
                c2 = Math.fma(a.getAsDouble(i * VECTORIZATION + 1), a.getAsDouble(i * VECTORIZATION + 1) * weights.getAsDouble(i * VECTORIZATION + 1), c2)
                c3 = Math.fma(a.getAsDouble(i * VECTORIZATION + 2), a.getAsDouble(i * VECTORIZATION + 2) * weights.getAsDouble(i * VECTORIZATION + 2), c3)
                c4 = Math.fma(a.getAsDouble(i * VECTORIZATION + 3), a.getAsDouble(i * VECTORIZATION + 3) * weights.getAsDouble(i * VECTORIZATION + 3), c4)

                d1 = Math.fma(b.getAsDouble(i * VECTORIZATION), b.getAsDouble(i * VECTORIZATION) * weights.getAsDouble(i * VECTORIZATION), d1)
                d2 = Math.fma(b.getAsDouble(i * VECTORIZATION + 1), b.getAsDouble(i * VECTORIZATION + 1) * weights.getAsDouble(i * VECTORIZATION + 1), d2)
                d3 = Math.fma(b.getAsDouble(i * VECTORIZATION + 2), b.getAsDouble(i * VECTORIZATION + 2) * weights.getAsDouble(i * VECTORIZATION + 2), d3)
                d4 = Math.fma(b.getAsDouble(i * VECTORIZATION + 3), b.getAsDouble(i * VECTORIZATION + 3) * weights.getAsDouble(i * VECTORIZATION + 3), d4)
            }
            val div = sqrt(c1 + c2 + c3 + c4) * sqrt(d1 + d2 + d3 + d4)
            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - (dot1 + dot2 + dot3 + dot4) / div
            }
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, shape: Shape): Double {
            var dot1 = 0.0
            var dot2 = 0.0
            var dot3 = 0.0
            var dot4 = 0.0

            var c1 = 0.0
            var c2 = 0.0
            var c3 = 0.0
            var c4 = 0.0

            var d1 = 0.0
            var d2 = 0.0
            var d3 = 0.0
            var d4 = 0.0

            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                dot1 = Math.fma(a.getAsDouble(i * VECTORIZATION), b.getAsDouble(i * VECTORIZATION), dot1)
                dot2 = Math.fma(a.getAsDouble(i * VECTORIZATION + 1), b.getAsDouble(i * VECTORIZATION + 1), dot2)
                dot3 = Math.fma(a.getAsDouble(i * VECTORIZATION + 2), b.getAsDouble(i * VECTORIZATION + 2), dot3)
                dot4 = Math.fma(a.getAsDouble(i * VECTORIZATION + 3), b.getAsDouble(i * VECTORIZATION + 3), dot4)

                c1 = Math.fma(a.getAsDouble(i * VECTORIZATION), a.getAsDouble(i * VECTORIZATION), c1)
                c2 = Math.fma(a.getAsDouble(i * VECTORIZATION + 1), a.getAsDouble(i * VECTORIZATION + 1), c2)
                c3 = Math.fma(a.getAsDouble(i * VECTORIZATION + 2), a.getAsDouble(i * VECTORIZATION + 2), c3)
                c4 = Math.fma(a.getAsDouble(i * VECTORIZATION + 3), a.getAsDouble(i * VECTORIZATION + 3), c4)

                d1 = Math.fma(b.getAsDouble(i * VECTORIZATION), b.getAsDouble(i * VECTORIZATION), d1)
                d2 = Math.fma(b.getAsDouble(i * VECTORIZATION + 1), b.getAsDouble(i * VECTORIZATION + 1), d2)
                d3 = Math.fma(b.getAsDouble(i * VECTORIZATION + 2), b.getAsDouble(i * VECTORIZATION + 2), d3)
                d4 = Math.fma(b.getAsDouble(i * VECTORIZATION + 3), b.getAsDouble(i * VECTORIZATION + 3), d4)
            }
            val div = sqrt(c1 + c2 + c3 + c4) * sqrt(d1 + d2 + d3 + d4)
            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - (dot1 + dot2 + dot3 + dot4) / div
            }
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0
            for (i in 0 until b.size) {
                dot += a.getAsDouble(i) * b.getAsDouble(i) * weights.getAsDouble(i)
                c += a.getAsDouble(i) * a.getAsDouble(i) * weights.getAsDouble(i)
                d += b.getAsDouble(i) * b.getAsDouble(i) * weights.getAsDouble(i)
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0
            for (i in 0 until b.size) {
                dot += a.getAsDouble(i) * b.getAsDouble(i)
                c += a.getAsDouble(i) * a.getAsDouble(i)
                d += b.getAsDouble(i) * b.getAsDouble(i)
            }
            val div = sqrt(c) * sqrt(d)

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
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double = b.indices.mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else weights.getAsDouble(i) }.sum()
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double = b.indices.mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0 }.sum()
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

        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>, weights: VectorValue<*>): Double = this.haversine(a.getAsDouble(0), a.getAsDouble(1), b.getAsDouble(0), b.getAsDouble(1))
        override fun invoke(a: VectorValue<DoubleArray>, b: VectorValue<DoubleArray>): Double = this.haversine(a.getAsDouble(0), a.getAsDouble(1), b.getAsDouble(0), b.getAsDouble(1))

        /**
         * Calculates the haversine distance of two spherical coordinates in degrees.
         *
         * @param a_lat Start coordinate (latitude) in degrees.
         * @param a_lon Start coordinate (longitude) in degrees.
         * @param b_lat End coordinate (latitude) in degrees.
         * @param b_lon End coordinate (longitude) in degrees.

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
    };

    companion object {
        private const val VECTORIZATION = 4
    }
}
