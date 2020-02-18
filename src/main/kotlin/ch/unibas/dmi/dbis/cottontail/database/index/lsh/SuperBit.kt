package ch.unibas.dmi.dbis.cottontail.database.index.lsh

import java.io.Serializable
import java.util.*
import kotlin.math.sqrt

/**
 * Implementation of Super-Bit Locality-Sensitive Hashing.
 * Super-Bit is an improvement of Random Projection LSH. It computes an estimation of cosine similarity.
 * <p>
 * Super-Bit Locality-Sensitive Hashing
 * Jianqiu Ji, Jianmin Li, Shuicheng Yan, Bo Zhang, Qi Tian
 * http://papers.nips.cc/paper/4847-super-bit-locality-sensitive-hashing.pdf
 * <p>
 * This class is inspired by Thibault Debatty (https://github.com/tdebatty/java-LSH).
 *
 * @param d     dimension, R^n
 * @param N     Super-Bit depth (must be [1 .. d])
 * @param L     number of Super-Bits (must be [1 ..)
 * @param seed  to use for the random number generator
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class SuperBit(d: Int, N: Int, L: Int, seed: Int) : Serializable {

    private lateinit var hyperplanes: Array<DoubleArray>

    init {
        this(d, N, L, Random(seed.toLong()))
    }

    /**
     * The K vectors are orthogonalized in L batches of N vectors.
     * <p>
     * Super-Bit depth N must be [1 .. d]
     * Number of Super-Bits L must be [1 ..
     * The resulting code length is K = N * L (size of signature)
     *
     * @param d         dimension, R^n
     * @param N         Super-Bit depth (must be [1 .. d])
     * @param L         number of Super-Bits (must be [1 ..)
     * @param random    to use for the random number generator
     */
    private operator fun invoke(d: Int, N: Int, L: Int, random: Random) {
        require(d > 0) { "Dimension d must be >= 1" }
        require(!(N < 1 || N > d)) { "Super-Bit depth N must be 1 <= N <= d" }
        require(L >= 1) { "Number of Super-Bits L must be >= 1" }

        /*
        -----------------------------------------------------------------------------------------------
        Algorithm 1 - Generating Super-Bit Locality-Sensitive Hashing Projection Vectors
        -----------------------------------------------------------------------------------------------
        Input: Data space dimension d, Super-Bit depth 1 <= N <= d, number of Super-Bit L >= 1,
        resulting code length K = N * L.

        Generate a random matrix H with each element sampled independently from the normal distribution
        N (0, 1), with each column normalized to unit length. Denote H = [v1, v2, ..., vK].

        Output: H ̃ = [w ,w ,...,w ].
        */

        val K = N * L

        val v = Array(K) { DoubleArray(d * 2) } // H

        for (i in 0 until K) {
            for (j in 0 until d * 2) {
                v[i][j] = random.nextGaussian()
            }
            normalize(v[i])
        }

        val w = Array(K) { DoubleArray(d * 2) }

        for (i in 0 until L) {
            for (j in 1..N) {
                System.arraycopy(v[i * N + j - 1], 0, w[i * N + j - 1], 0, d * 2)
                for (k in 1 until j) {
                    w[i * N + j - 1] =
                            difference(
                                    w[i * N + j - 1],
                                    product(
                                            dotProduct(
                                                    w[i * N + k - 1],
                                                    v[i * N + j - 1]),
                                            w[i * N + k - 1]))
                }
                normalize(w[i * N + j - 1])
            }
        }

        hyperplanes = w // H ̃
    }

    /**
     * Compute the signature of a vector.
     *
     * @param vector
     * @return The signature.
     */
    fun signature(vector: DoubleArray): BooleanArray {
        val signature = BooleanArray(hyperplanes.size)
        for (i in hyperplanes.indices) {
            signature[i] = complexDotProduct(hyperplanes[i], vector) >= 0
        }
        return signature
    }

    /**
     * Normalize a vector.
     *
     * @param vector
     */
    private fun normalize(vector: DoubleArray) {
        val norm: Double = norm(vector)
        for (i in vector.indices) {
            vector[i] = vector[i] / norm
        }
    }

    /**
     * Calculate the norm L2 (sqrt(sum_i(v_i^2))).
     *
     * @param vector
     * @return The norm L2.
     */
    private fun norm(vector: DoubleArray): Double {
        var sum = 0.0
        for (i in vector.indices) {
            sum += vector[i] * vector[i]
        }
        return sqrt(sum)
    }

    /**
     * Calculate the difference of two vectors.
     *
     * @param vector1
     * @param vector2
     * @return The difference.
     */
    private fun difference(vector1: DoubleArray, vector2: DoubleArray): DoubleArray {
        val difference = DoubleArray(vector1.size)
        for (i in vector1.indices) {
            difference[i] = vector1[i] - vector2[i]
        }
        return difference
    }

    /**
     * Calculate the product of two vectors.
     *
     * @param vector1
     * @param vector2
     * @return The product.
     */
    private fun product(vector1: Double, vector2: DoubleArray): DoubleArray {
        val product = DoubleArray(vector2.size)
        for (i in vector2.indices) {
            product[i] = vector1 * vector2[i]
        }
        return product
    }

    /**
     * Calculate the dot product of two vectors.
     *
     * @param vector1
     * @param vector2
     * @return The dot product.
     */
    private fun dotProduct(vector1: DoubleArray, vector2: DoubleArray): Double {
        var dotProduct = 0.0
        for (i in vector1.indices) {
            dotProduct += vector1[i] * vector2[i]
        }
        return dotProduct
    }

    /**
     * Calculate the dot product of two complex vectors.
     *
     * @param vector1
     * @param vector2
     * @return The dot product.
     */
    private fun complexDotProduct(vector1: DoubleArray, vector2: DoubleArray): Double {
        var dotProduct = 0.0
        for (i in 0 until vector2.size / 2) {
            dotProduct += vector1[i * 2] * vector2[i * 2] + vector1[i * 2 + 1] * vector2[i * 2 + 1]
        }
        return dotProduct
    }

}