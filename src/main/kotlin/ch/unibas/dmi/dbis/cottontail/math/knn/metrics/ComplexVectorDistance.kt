package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.ComplexVectorValue
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
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>, shape: Shape): Double {
            // TODO
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
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
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>, shape: Shape): Double {
            // TODO
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
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
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            // TODO
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
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, shape: Shape): Double {
            // TODO
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>, weights: VectorValue<*>): Double {
            // TODO
            return 0.0
        }

        override fun invoke(a: VectorValue<Array<Complex>>, b: VectorValue<Array<Complex>>): Double {
            var dot = 0.0
            var c = 0.0
            var d = 0.0
            // TODO (for now only the real part is compared)
            for (i in b.value.indices) {
                dot += a.value[i][0] * b.value[i][0]
                c += a.value[i][0] * a.value[i][0]
                d += b.value[i][0] * b.value[i][0]
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