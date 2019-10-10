package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex
import kotlin.math.*

enum class ComplexVectorDistance : VectorizedDistanceFunction<Array<Complex>> {

    /**
     * L1 or Manhattan distance between two vectors. Vectors must be of the same size!
     */
    L1 {
        override val operations: Int = 1

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, shape: Shape): Double {
            // TODO
            /*var sum1 = 0.0
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
            return sum1 + sum2 + sum3 + sum4*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>, shape: Shape): Double {
            // TODO
            /*var sum1 = 0.0
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
            return sum1 + sum2 + sum3 + sum4*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            /*var sum = 0.0
            for (i in b.indices) {
                sum += abs(b.getAsDouble(i) - a.getAsDouble(i)) * weights.getAsDouble(i)
            }
            return sum*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
            /*var sum = 0.0
            for (i in b.indices) {
                sum += abs(b.getAsDouble(i) - a.getAsDouble(i))
            }
            return sum*/
            return 0.0
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override val operations: Int = 2
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, shape: Shape): Double = sqrt(L2SQUARED(a, b, shape))
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>, shape: Shape): Double = sqrt(L2SQUARED(a, b, weights, shape))
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double = sqrt(L2SQUARED(a, b))
    },

    /**
     * Squared L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {
        override val operations: Int = 2

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, shape: Shape): Double {
            // TODO
            /*var sum1 = 0.0
            var sum2 = 0.0
            var sum3 = 0.0
            var sum4 = 0.0
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                sum1 += (b.getAsFloat(i * VECTORIZATION) - a.getAsFloat(i * VECTORIZATION)).toDouble().pow(2.0)
                sum2 += (b.getAsFloat(i * VECTORIZATION + 1) - a.getAsFloat(i * VECTORIZATION + 1)).toDouble().pow(2.0)
                sum3 += (b.getAsFloat(i * VECTORIZATION + 2) - a.getAsFloat(i * VECTORIZATION + 2)).toDouble().pow(2.0)
                sum4 += (b.getAsFloat(i * VECTORIZATION + 3) - a.getAsFloat(i * VECTORIZATION + 3)).toDouble().pow(2.0)
            }
            for (i in max * VECTORIZATION until b.size) {
                sum4 += (b.getAsFloat(i) - a.getAsFloat(i)).toDouble().pow(2.0)
            }
            return (sum1 + sum2 + sum3 + sum4)*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>, shape: Shape): Double {
            // TODO
            /*var sum1 = 0.0f
            var sum2 = 0.0f
            var sum3 = 0.0f
            var sum4 = 0.0f
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                sum1 = Math.fma(b.getAsFloat(i * VECTORIZATION) - a.getAsFloat(i * VECTORIZATION), (b.getAsFloat(i * VECTORIZATION) - a.getAsFloat(i * VECTORIZATION)) * weights.getAsFloat(i * VECTORIZATION), sum1)
                sum2 = Math.fma(b.getAsFloat(i * VECTORIZATION + 1) - a.getAsFloat(i * VECTORIZATION + 1), (b.getAsFloat(i * VECTORIZATION + 1) - a.getAsFloat(i * VECTORIZATION + 1)) * weights.getAsFloat(i * VECTORIZATION + 1), sum2)
                sum3 = Math.fma(b.getAsFloat(i * VECTORIZATION + 2) - a.getAsFloat(i * VECTORIZATION + 2), (b.getAsFloat(i * VECTORIZATION + 2) - a.getAsFloat(i * VECTORIZATION + 2)) * weights.getAsFloat(i * VECTORIZATION + 2), sum3)
                sum4 = Math.fma(b.getAsFloat(i * VECTORIZATION + 3) - a.getAsFloat(i * VECTORIZATION + 3), (b.getAsFloat(i * VECTORIZATION + 3) - a.getAsFloat(i * VECTORIZATION + 3) * weights.getAsFloat(i * VECTORIZATION + 3)), sum4)
            }
            for (i in max * VECTORIZATION until b.size) {
                sum4 = Math.fma(b.getAsFloat(i) - a.getAsFloat(i), (b.getAsFloat(i) - a.getAsFloat(i)) * weights.getAsFloat(i), sum4)
            }
            return (sum1 + sum2 + sum3 + sum4).toDouble()*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            /*var sum = 0.0f
            for (i in b.indices) {
                sum += (b.getAsFloat(i) - a.getAsFloat(i)) * (b.getAsFloat(i) - a.getAsFloat(i)) * weights.getAsFloat(i)
            }
            return sum.toDouble()*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
            /*var sum = 0.0f
            for (i in b.indices) {
                sum += (b.getAsFloat(i) - a.getAsFloat(i)) * (b.getAsFloat(i) - a.getAsFloat(i))
            }
            return sum.toDouble()*/
            return 0.0
        }
    },

    /**
     * Chi Squared distance between two vectors. Vectors must be of the same size!
     */
    CHISQUARED {
        override val operations: Int = 3

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            /*var sum = 0.0f
            for (i in b.indices) {
                if (abs(a.getAsFloat(i) + b.getAsFloat(i)) > 1e-6) {
                    sum += ((b.getAsFloat(i) - a.getAsFloat(i)) * (b.getAsFloat(i) - a.getAsFloat(i))) / (b.getAsFloat(i) + a.getAsFloat(i)) * weights.getAsFloat(i)
                }
            }
            return sum.toDouble()*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
            /*var sum = 0.0f
            for (i in b.indices) {
                if (abs(a.getAsFloat(i) + b.getAsFloat(i)) > 1e-6) {
                    sum += ((b.getAsFloat(i) - a.getAsFloat(i)) * (b.getAsFloat(i) - a.getAsFloat(i))) / (b.getAsFloat(i) + a.getAsFloat(i))
                }
            }
            return sum.toDouble()*/
            return 0.0
        }
    },

    /**
     * Cosine distance between two vectors. Vectors must be of the same size!
     */
    COSINE {
        override val operations: Int = 3

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>, shape: Shape): Double {
            // TODO
            /*var dot1 = 0.0f
            var dot2 = 0.0f
            var dot3 = 0.0f
            var dot4 = 0.0f

            var c1 = 0.0f
            var c2 = 0.0f
            var c3 = 0.0f
            var c4 = 0.0f

            var d1 = 0.0f
            var d2 = 0.0f
            var d3 = 0.0f
            var d4 = 0.0f

            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                dot1 = Math.fma(a.getAsFloat(i * VECTORIZATION), b.getAsFloat(i * VECTORIZATION) * weights.getAsFloat(i * VECTORIZATION), dot1)
                dot2 = Math.fma(a.getAsFloat(i * VECTORIZATION + 1), b.getAsFloat(i * VECTORIZATION + 1) * weights.getAsFloat(i * VECTORIZATION + 1), dot2)
                dot3 = Math.fma(a.getAsFloat(i * VECTORIZATION + 2), b.getAsFloat(i * VECTORIZATION + 2) * weights.getAsFloat(i * VECTORIZATION + 2), dot3)
                dot4 = Math.fma(a.getAsFloat(i * VECTORIZATION + 3), b.getAsFloat(i * VECTORIZATION + 3) * weights.getAsFloat(i * VECTORIZATION + 3), dot4)

                c1 = Math.fma(a.getAsFloat(i * VECTORIZATION), a.getAsFloat(i * VECTORIZATION) * weights.getAsFloat(i * VECTORIZATION), c1)
                c2 = Math.fma(a.getAsFloat(i * VECTORIZATION + 1), a.getAsFloat(i * VECTORIZATION + 1) * weights.getAsFloat(i * VECTORIZATION + 1), c2)
                c3 = Math.fma(a.getAsFloat(i * VECTORIZATION + 2), a.getAsFloat(i * VECTORIZATION + 2) * weights.getAsFloat(i * VECTORIZATION + 2), c3)
                c4 = Math.fma(a.getAsFloat(i * VECTORIZATION + 3), a.getAsFloat(i * VECTORIZATION + 3) * weights.getAsFloat(i * VECTORIZATION + 3), c4)

                d1 = Math.fma(b.getAsFloat(i * VECTORIZATION), b.getAsFloat(i * VECTORIZATION) * weights.getAsFloat(i * VECTORIZATION), d1)
                d2 = Math.fma(b.getAsFloat(i * VECTORIZATION + 1), b.getAsFloat(i * VECTORIZATION + 1) * weights.getAsFloat(i * VECTORIZATION + 1), d2)
                d3 = Math.fma(b.getAsFloat(i * VECTORIZATION + 2), b.getAsFloat(i * VECTORIZATION + 2) * weights.getAsFloat(i * VECTORIZATION + 2), d3)
                d4 = Math.fma(b.getAsFloat(i * VECTORIZATION + 3), b.getAsFloat(i * VECTORIZATION + 3) * weights.getAsFloat(i * VECTORIZATION + 3), d4)
            }
            val div = sqrt((c1 + c2 + c3 + c4).toDouble()) * sqrt((d1 + d2 + d3 + d4).toDouble())
            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - (dot1 + dot2 + dot3 + dot4) / div
            }*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, shape: Shape): Double {
            // TODO
            /*var dot1 = 0.0f
            var dot2 = 0.0f
            var dot3 = 0.0f
            var dot4 = 0.0f

            var c1 = 0.0f
            var c2 = 0.0f
            var c3 = 0.0f
            var c4 = 0.0f

            var d1 = 0.0f
            var d2 = 0.0f
            var d3 = 0.0f
            var d4 = 0.0f

            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                dot1 = Math.fma(a.getAsFloat(i * VECTORIZATION), b.getAsFloat(i * VECTORIZATION), dot1)
                dot2 = Math.fma(a.getAsFloat(i * VECTORIZATION + 1), b.getAsFloat(i * VECTORIZATION + 1), dot2)
                dot3 = Math.fma(a.getAsFloat(i * VECTORIZATION + 2), b.getAsFloat(i * VECTORIZATION + 2), dot3)
                dot4 = Math.fma(a.getAsFloat(i * VECTORIZATION + 3), b.getAsFloat(i * VECTORIZATION + 3), dot4)

                c1 = Math.fma(a.getAsFloat(i * VECTORIZATION), a.getAsFloat(i * VECTORIZATION), c1)
                c2 = Math.fma(a.getAsFloat(i * VECTORIZATION + 1), a.getAsFloat(i * VECTORIZATION + 1), c2)
                c3 = Math.fma(a.getAsFloat(i * VECTORIZATION + 2), a.getAsFloat(i * VECTORIZATION + 2), c3)
                c4 = Math.fma(a.getAsFloat(i * VECTORIZATION + 3), a.getAsFloat(i * VECTORIZATION + 3), c4)

                d1 = Math.fma(b.getAsFloat(i * VECTORIZATION), b.getAsFloat(i * VECTORIZATION), d1)
                d2 = Math.fma(b.getAsFloat(i * VECTORIZATION + 1), b.getAsFloat(i * VECTORIZATION + 1), d2)
                d3 = Math.fma(b.getAsFloat(i * VECTORIZATION + 2), b.getAsFloat(i * VECTORIZATION + 2), d3)
                d4 = Math.fma(b.getAsFloat(i * VECTORIZATION + 3), b.getAsFloat(i * VECTORIZATION + 3), d4)
            }
            val div = sqrt((c1 + c2 + c3 + c4).toDouble()) * sqrt((d1 + d2 + d3 + d4).toDouble())
            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - (dot1 + dot2 + dot3 + dot4) / div
            }*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            /*var dot = 0.0f
            var c = 0.0f
            var d = 0.0f
            for (i in 0 until b.size) {
                dot += a.getAsFloat(i) * b.getAsFloat(i) * weights.getAsFloat(i)
                c += a.getAsFloat(i) * a.getAsFloat(i) * weights.getAsFloat(i)
                d += b.getAsFloat(i) * b.getAsFloat(i) * weights.getAsFloat(i)
            }
            val div = sqrt(c.toDouble()) * sqrt(d.toDouble())
            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }*/
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
            /*var dot = 0.0
            var c = 0.0
            var d = 0.0
            for (i in 0 until b.size) {
                dot += a.getAsFloat(i) * b.getAsFloat(i)
                c += a.getAsFloat(i) * a.getAsFloat(i)
                d += b.getAsFloat(i) * b.getAsFloat(i)
            }
            val div = sqrt(c) * sqrt(d)
            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }*/
            return 0.0
        }
    },

    /**
     * Hamming distance: Makes an element wise comparison of the two arrays and increases the distance by 1, everytime two corresponding elements don't match.
     */
    HAMMING {
        override val operations: Int = 1

        // TODO
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double = b.indices.mapIndexed { i, _ -> if (b[i] == a[i]) 0.0f else weights.getAsFloat(i) }.sum().toDouble()

        // TODO
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double = b.indices.mapIndexed { i, _ -> if (b[i] == a[i]) 0.0 else 1.0 }.sum()
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

        // TODO
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double = this.haversine(a.getAsDouble(0), a.getAsDouble(1), b.getAsDouble(0), b.getAsDouble(1))

        // TODO
        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double = this.haversine(a.getAsDouble(0), a.getAsDouble(1), b.getAsDouble(0), b.getAsDouble(1))

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