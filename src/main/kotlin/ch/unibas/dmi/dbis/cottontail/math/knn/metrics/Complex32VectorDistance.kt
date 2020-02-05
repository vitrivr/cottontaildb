package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex32
import kotlin.math.*

/**
 * Collection of distance  [VectorizedDistanceFunction] for complex, single precision vectors.
 *
 * @see Complex64VectorValue
 *
 * @author Manuel Hürbin & Ralph Gasser
 * @version 1.1
 */
enum class Complex32VectorDistance : VectorizedDistanceFunction<FloatArray> {
    /**
     * L1 (Manhattan) distance between two vectors. Vectors must be of the same size!
     */
    L1 {
        override val operations: Int = 1

        /**
         * Weighted L1 (Manhattan) distance between two complex vectors (vectorized). Use following identity:
         *
         * L1:
         *   d = sum(d_i)
         *   d_i = |(b_i-a_i)| for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = sqrt(z^2) = sqrt((b1-a1)^2 - (b2-a2)^2 + 2(b1-a1)(b2-a2))
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, shape: Shape): Double {
            val sum = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val re = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val im = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            var idx: Int
            val max = Math.floorDiv(a.size, VECTORIZATION)
            for (i in 0 until max) {
                idx = i * VECTORIZATION
                re[0] = b.getAsFloat(2 * idx) - a.getAsFloat(2 * idx)
                re[1] = b.getAsFloat(2 * idx + 2) - a.getAsFloat(2 * idx + 2)
                re[2] = b.getAsFloat(2 * idx + 4) - a.getAsFloat(2 * idx + 4)
                re[3] = b.getAsFloat(2 * idx + 6) - a.getAsFloat(2 * idx + 6)
                im[0] = b.getAsFloat(2 * idx + 1) - a.getAsFloat(2 * idx + 1)
                im[1] = b.getAsFloat(2 * idx + 3) - a.getAsFloat(2 * idx + 3)
                im[2] = b.getAsFloat(2 * idx + 5) - a.getAsFloat(2 * idx + 5)
                im[3] = b.getAsFloat(2 * idx + 7) - a.getAsFloat(2 * idx + 7)

                sum[0] += sqrt(re[0].pow(2) - im[0].pow(2) + 2 * re[0] * im[0])
                sum[1] += sqrt(re[1].pow(2) - im[1].pow(2) + 2 * re[1] * im[1])
                sum[2] += sqrt(re[2].pow(2) - im[2].pow(2) + 2 * re[2] * im[2])
                sum[3] += sqrt(re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3])
            }
            for (i in max * VECTORIZATION until b.size) {
                re[3] = b.getAsFloat(2 * i) - a.getAsFloat(2 *i)
                im[3] = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum[3] += sqrt(re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3])
            }
            return sum.sum().toDouble()
        }

        /**
         * Weighted L1 (Manhattan) distance between two complex vectors (vectorized). Use following identity:
         *
         * L1:
         *   d = sum(d_i)
         *   d_i = |(b_i-a_i)| for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = sqrt(z^2) = sqrt((b1-a1)^2 - (b2-a2)^2 + 2(b1-a1)(b2-a2))
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>, shape: Shape): Double {
            val sum = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val re = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val im = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val max = Math.floorDiv(a.size, VECTORIZATION)
            var idx: Int
            for (i in 0 until max) {
                idx = i * VECTORIZATION
                re[0] = b.getAsFloat(2 * idx) - a.getAsFloat(2 * idx)
                re[1] = b.getAsFloat(2 * idx + 2) - a.getAsFloat(2 * idx + 2)
                re[2] = b.getAsFloat(2 * idx + 4) - a.getAsFloat(2 * idx + 4)
                re[3] = b.getAsFloat(2 * idx + 6) - a.getAsFloat(2 * idx + 6)
                im[0] = b.getAsFloat(2 * idx + 1) - a.getAsFloat(2 * idx + 1)
                im[1] = b.getAsFloat(2 * idx + 3) - a.getAsFloat(2 * idx + 3)
                im[2] = b.getAsFloat(2 * idx + 5) - a.getAsFloat(2 * idx + 5)
                im[3] = b.getAsFloat(2 * idx + 7) - a.getAsFloat(2 * idx + 7)

                sum[0] += sqrt(re[0].pow(2) - im[0].pow(2) + 2 * re[0] * im[0]) * weights.getAsFloat(i)
                sum[1] += sqrt(re[1].pow(2) - im[1].pow(2) + 2 * re[1] * im[1]) * weights.getAsFloat(i + 1)
                sum[2] += sqrt(re[2].pow(2) - im[2].pow(2) + 2 * re[2] * im[2]) * weights.getAsFloat(i + 2)
                sum[3] += sqrt(re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3]) * weights.getAsFloat(i + 3)
            }
            for (i in max * VECTORIZATION until b.size) {
                re[3] = b.getAsFloat(2 * i) - a.getAsFloat(2 * i)
                im[3] = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum[3] += sqrt(re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3]) * weights.getAsFloat(i)
            }
            return sum.sum().toDouble()
        }

        /**
         * Weighted L1 (Manhattan) distance between two complex vectors. Use following identity:
         *
         * L1:
         *   d = sum(d_i)
         *   d_i = |(b_i-a_i)| for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = sqrt(z^2) = sqrt((b1-a1)^2 - (b2-a2)^2 + 2(b1-a1)(b2-a2))
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>): Double {
            var sum = 0.0f
            var re: Float /* Real part of (b-a). */
            var im: Float /* Imaginary part of (b-a). */
            for (i in 0 until b.size) {
                re = b.getAsFloat(2 * i) - a.getAsFloat(2 * i)
                im = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum += sqrt(re.pow(2) - im.pow(2) + 2 * re * im)  * weights.getAsFloat(i) /* |b-a| . w */
            }
            return sum.toDouble()
        }


        /**
         * L1 (Manhattan) distance between two complex vectors. Use following identity:
         *
         * L1:
         *   d = sum(d_i)
         *   d_i = |(b_i-a_i)| for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = sqrt(z^2) = sqrt((b1-a1)^2 - (b2-a2)^2 + 2(b1-a1)(b2-a2))
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>): Double {
            var sum = 0.0f
            var re: Float /* Real part of (b-a). */
            var im: Float /* Imaginary part of (b-a). */
            for (i in 0 until b.size) {
                re = b.getAsFloat(2 * i) - a.getAsFloat( 2 * i)
                im = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum += sqrt(re.pow(2) - im.pow(2) + 2 * re * im)  /* |b-a| */
            }
            return sum.toDouble()
        }
    },

    /**
     * L2 (Euclidian) distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override val operations: Int = 2
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, shape: Shape): Double = sqrt(L2SQUARED(a, b, shape))
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>, shape: Shape): Double = sqrt(L2SQUARED(a, b, weights, shape))
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>): Double = sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>): Double = sqrt(L2SQUARED(a, b))
    },

    /**
     * Squared L2 (Euclidian) distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {
        override val operations: Int = 2

        /**
         * Squared L2 (Euclidian) distance between two complex vectors (vectorized). Use following identity:
         *
         * L2 (squared):
         *   d = sum(d_i)
         *   d_i = (b_i-a_i)^2 for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = z1^2-z2^2+2*z1*z2 = (b1-a1)^2 - (b2-a2)^2 + 2*(b1-a1)*(b2-a2)
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, shape: Shape): Double {
            val sum = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val re = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val im = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val max = Math.floorDiv(a.size, VECTORIZATION)
            var idx: Int
            for (i in 0 until max) {
                idx = i * VECTORIZATION
                re[0] = b.getAsFloat(2 * idx) - a.getAsFloat(2 * idx)
                re[1] = b.getAsFloat(2 * idx + 2) - a.getAsFloat(2 * idx + 2)
                re[2] = b.getAsFloat(2 * idx + 4) - a.getAsFloat(2 * idx + 4)
                re[3] = b.getAsFloat(2 * idx + 6) - a.getAsFloat(2 * idx + 6)
                im[0] = b.getAsFloat(2 * idx + 1) - a.getAsFloat(2 * idx + 1)
                im[1] = b.getAsFloat(2 * idx + 3) - a.getAsFloat(2 * idx + 3)
                im[2] = b.getAsFloat(2 * idx + 5) - a.getAsFloat(2 * idx + 5)
                im[3] = b.getAsFloat(2 * idx + 7) - a.getAsFloat(2 * idx + 7)

                sum[0] += re[0].pow(2) - im[0].pow(2) + 2 * re[0] * im[0]
                sum[1] += re[1].pow(2) - im[1].pow(2) + 2 * re[1] * im[1]
                sum[2] += re[2].pow(2) - im[2].pow(2) + 2 * re[2] * im[2]
                sum[3] += re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3]
            }
            for (i in max * VECTORIZATION until b.size) {
                re[3] = b.getAsFloat(2 * i) - a.getAsFloat(2 *i)
                im[3] = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum[3] +=re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3]
            }
            return sum.sum().toDouble()
        }

        /**
         * Squared, weighted L2 (Euclidian) distance between two complex vectors (vectorized). Use following identity:
         *
         * L2 (squared):
         *   d = sum(d_i)
         *   d_i = (b_i-a_i)^2 for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = z1^2-z2^2+2*z1*z2 = (b1-a1)^2 - (b2-a2)^2 + 2*(b1-a1)*(b2-a2)
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>, shape: Shape): Double {
            val sum = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val re = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val im = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            val max = Math.floorDiv(a.size, VECTORIZATION)
            var idx: Int
            for (i in 0 until max) {
                idx = i * VECTORIZATION
                re[0] = b.getAsFloat(2 * idx) - a.getAsFloat(2 * idx)
                re[1] = b.getAsFloat(2 * idx + 2) - a.getAsFloat(2 * idx + 2)
                re[2] = b.getAsFloat(2 * idx + 4) - a.getAsFloat(2 * idx + 4)
                re[3] = b.getAsFloat(2 * idx + 6) - a.getAsFloat(2 * idx + 6)
                im[0] = b.getAsFloat(2 * idx + 1) - a.getAsFloat(2 * idx + 1)
                im[1] = b.getAsFloat(2 * idx + 3) - a.getAsFloat(2 * idx + 3)
                im[2] = b.getAsFloat(2 * idx + 5) - a.getAsFloat(2 * idx + 5)
                im[3] = b.getAsFloat(2 * idx + 7) - a.getAsFloat(2 * idx + 7)

                sum[0] += (re[0].pow(2) - im[0].pow(2) + 2 * re[0] * im[0]) * weights.getAsFloat(idx)
                sum[1] += (re[1].pow(2) - im[1].pow(2) + 2 * re[1] * im[1]) * weights.getAsFloat(idx + 1)
                sum[2] += (re[2].pow(2) - im[2].pow(2) + 2 * re[2] * im[2]) * weights.getAsFloat(idx + 2)
                sum[3] += (re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3]) * weights.getAsFloat(idx + 3)
            }
            for (i in max * VECTORIZATION until b.size) {
                re[3] = b.getAsFloat(2 * i) - a.getAsFloat(2 *i)
                im[3] = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum[3] += (re[3].pow(2) - im[3].pow(2) + 2 * re[3] * im[3]) * weights.getAsFloat(i)
            }
            return sum.sum().toDouble()
        }

        /**
         * Squared, weighted L2 (Euclidian) distance between two complex vectors. Use following identity:
         *
         * L2 (squared):
         *   d = sum(d_i)
         *   d_i = (b_i-a_i)^2 for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = z1^2-z2^2+2*z1*z2 = (b1-a1)^2 - (b2-a2)^2 + 2*(b1-a1)*(b2-a2)
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>): Double {
            var sum = 0.0f
            var re: Float /* Real part of (b-a). */
            var im: Float /* Imaginary part of (b-a). */
            for (i in 0 until b.size) {
                re = b.getAsFloat(2 * i) - a.getAsFloat(2 * i)
                im = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum += (re.pow(2) - im.pow(2) + 2*re*im) * weights.getAsFloat(i)
            }
            return sum.toDouble()
        }


        /**
         * Squared L2 (Euclidian) distance between two complex vectors. Use following identity:
         *
         * L2 (squared):
         *   d = sum(d_i)
         *   d_i = (b_i-a_i)^2 for a_i,b_i \in C
         *
         * Substitute (per component): (b-a) = (b1-a1) + i*(b2-a2) = z
         * d_i = z1^2-z2^2+2*z1*z2 = (b1-a1)^2 - (b2-a2)^2 + 2*(b1-a1)*(b2-a2)
         */
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>): Double {
            var sum = 0.0f
            var re: Float /* Real part of (b-a). */
            var im: Float /* Imaginary part of (b-a). */
            for (i in 0 until b.size) {
                re = b.getAsFloat(2 * i) - a.getAsFloat(2 * i)
                im = b.getAsFloat(2 * i + 1) - a.getAsFloat(2 * i + 1)
                sum += re.pow(2) - im.pow(2) + 2*re*im
            }
            return sum.toDouble()
        }
    },

    /**
     * Cosine distance between two vectors. Vectors must be of the same size!
     * https://www.quora.com/What-is-dot-product-of-two-complex-numbers
     */
    COSINE {
        override val operations: Int = 3

        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>, shape: Shape): Double  = invoke(a, b, weights) /* TODO: Vectorize. */

        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, shape: Shape): Double = invoke(a, b) /* TODO: Vectorize. */

        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>): Double {
            var dot = 0.0f
            var c = 0.0f
            var d = 0.0f
            for (i in 0 until b.size) {
                dot += (a.getAsFloat(i * 2) * b.getAsFloat(i * 2) + a.getAsFloat(i * 2 + 1) * b.getAsFloat(i * 2 + 1)) * weights.getAsFloat(i)
                c += (a.getAsFloat(i * 2) * a.getAsFloat(i * 2) + a.getAsFloat(i * 2 + 1) * a.getAsFloat(i * 2 + 1)) * weights.getAsFloat(i)
                d += (b.getAsFloat(i * 2) * b.getAsFloat(i * 2) + b.getAsFloat(i * 2 + 1) * b.getAsFloat(i * 2 + 1)) * weights.getAsFloat(i)
            }
            val div = sqrt(c) * sqrt(d)

            return if (div < 1e-6 || div.isNaN()) {
                1.0
            } else {
                1.0 - dot / div
            }
        }

        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>): Double {
            var dot = 0.0f
            var c = 0.0f
            var d = 0.0f
            for (i in 0 until b.size) {
                dot += a.getAsFloat(i * 2) * b.getAsFloat(i * 2) + a.getAsFloat(i * 2 + 1) * b.getAsFloat(i * 2 + 1)
                c += a.getAsFloat(i * 2) * a.getAsFloat(i * 2) + a.getAsFloat(i * 2 + 1) * a.getAsFloat(i * 2 + 1)
                d += b.getAsFloat(i * 2) * b.getAsFloat(i * 2) + b.getAsFloat(i * 2 + 1) * b.getAsFloat(i * 2 + 1)
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
     * Hamming distance: Makes an element wise comparison of the two arrays and increases the distance by 1, every time two corresponding elements don't match. For complex vectors, a pair of entries in the underlying double array must
     * be equal in order for the component to be considered a match.
     */
    HAMMING {
        override val operations: Int = 1
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>, weights: VectorValue<*>): Double = (0 until b.size).map { i -> if (b[i] == a[i] && b[i+1] == a[i+1]) 0.0f else weights.getAsFloat(i) }.sum().toDouble()
        override fun invoke(a: VectorValue<FloatArray>, b: VectorValue<FloatArray>): Double = (0 until b.size).map { i -> if (b[i] == a[i] && b[i+1] == a[i+1]) 0.0f else 1.0f }.sum().toDouble()
    };

    companion object  {
        private const val VECTORIZATION = 4
    }
}