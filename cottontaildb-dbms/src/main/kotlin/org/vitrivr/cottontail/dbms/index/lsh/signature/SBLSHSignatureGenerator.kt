package org.vitrivr.cottontail.dbms.index.lsh.signature

import org.vitrivr.cottontail.core.types.ComplexVectorValue
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import java.util.*

/**
 * LSH implementation relying on Super-Bit (SB), to bin vectors s times (stages)  in b buckets (per stage),
 * in a space with d dimensions. Input vectors  with a high cosine similarity have a high probability
 * of falling in the same bucket.
 *
 * @param stages    stages
 * @param buckets    buckets (per stage)
 * @param seed random number generator seed (sing the same value will guarantee identical hashes across object
 *             instantiations)
 *
 * This class is inspired by Thibault Debatty (https://github.com/tdebatty/java-LSH).
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.2.0
 */
class SBLSHSignatureGenerator(private val stages: Int, private val buckets: Int, private val seed: Long, private val dimension: Int) : LSHSignatureGenerator {

    companion object {
        const val LARGE_PRIME: Long = 433494437
    }

    /** The number of*/
    private val K = this.stages * this.buckets / 2

    /** The super bit depth. */
    private val N = computeSuperBitDepth(dimension, K)

    /** */
    private val random = SplittableRandom(this.seed)

    /** */
    private var superBit: SuperBit? = null

    /**
     * Compute the Super-Bit depth N.
     *
     * @param d The dimension of the vector.
     * @return Super-Bit depth N.
     */
    private fun computeSuperBitDepth(d: Int, k: Int): Int {
        var N: Int = d
        while (N >= 1) {
            if (k % N == 0) {
                break
            }
            N--
        }
        require(N != 0) { "Super-Bit depth must not be 0" }
        return N
    }

    /**
     * Hash a double[] vector in s stages into b buckets.
     *
     * @param vector
     * @return A int[] with the signature.
     */
    override fun train(vectors: Sequence<VectorValue<*>>) {
        this.superBit = SuperBit(vectors.first())
    }

    /**
     * Hashes a [VectorValue] into a [LSHSignature].
     *
     * @param vector The [VectorValue] to obtain the [LSHSignature].
     * @return A [LSHSignature].
     */
    override fun generate(vector: VectorValue<*>): LSHSignature {
        val sb = this.superBit ?: throw IllegalStateException("SuperBitLSHGenerator has not been initialized and can therefore not be used.")
        return if (vector is ComplexVectorValue) {
            hashSignature(sb.signatureComplex(vector))
        } else {
            hashSignature(sb.signature(vector))
        }
    }

    /**
     * Hash a signature. The signature is divided in s stages. Each stage is hashed to one of the b buckets.
     *
     * @param signatureReal Real-valued part of the signature.
     * @param signatureComplex Complex part of the signature.
     * @return A vector of s integers (between 0 and b - 1)
     */
    private fun hashSignature(signatureReal: BooleanArray, signatureComplex: BooleanArray? = null): LSHSignature {
        // create an accumulator for each stage
        // is this hashing locality preserving?
        require(signatureReal.size == K)
        val acc = LongArray(stages)
        for (i in 0 until stages) {
            acc[i] = 0
        }
        // number of rows per stage
        val rows = signatureReal.size / stages
        for (i in signatureReal.indices) {
            var j: Long = 0
            if (signatureReal[i]) j = (i + 1) * LARGE_PRIME
            // todo: Is this modification killing the hamming distance preserving features of this method (if it was there to begin with)?
            if (signatureComplex != null && signatureComplex[i]) j += (i + 1) * LARGE_PRIME
            // current stage
            val k = (i / rows).coerceAtMost(stages - 1)
            acc[k] = (acc[k] + j) % Int.MAX_VALUE
        }
        val vector = IntArray(stages)
        for (i in 0 until stages) {
            vector[i] = acc[i].toInt() % buckets
        }
        return LSHSignature(vector)
    }

    /**
     *
     */
    inner class SuperBit(species: VectorValue<*>) {
        /** List of hyperplanes held by this [SuperBit]. */
        private val hyperplanes: Array<VectorValue<*>>

        init {
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

            /* Sanity checks. */
            require(this@SBLSHSignatureGenerator.dimension > 0) { "Dimension d must be >= 1" }
            require(!(this@SBLSHSignatureGenerator.N < 1 || this@SBLSHSignatureGenerator.N > this@SBLSHSignatureGenerator.dimension)) { "Super-Bit depth N must be 1 <= N <= d" }
            val L = K / N
            require(L >= 1) { "Number of Super-Bits L must be >= 1" }

            val v = Array(this@SBLSHSignatureGenerator.K) {
                val rnd = when (species) { // centering components at 0 (subtract 0.5) looks more favorable in terms of hamming dist vs. cosine dist plots
                    is DoubleVectorValue -> DoubleVectorValue(DoubleArray(species.logicalSize) { random.nextDouble() - 0.5 })
                    is FloatVectorValue -> FloatVectorValue(FloatArray(species.logicalSize) { (random.nextDouble() - 0.5).toFloat() })
                    is Complex32VectorValue -> Complex32VectorValue(FloatArray(species.logicalSize * 2) { (random.nextDouble() - 0.5).toFloat() })
                    is Complex64VectorValue -> Complex64VectorValue(DoubleArray(species.logicalSize * 2) { random.nextDouble() - 0.5 })
                    else -> throw IllegalArgumentException("Unsupported vector type")
                } as VectorValue<*>
                rnd / rnd.norm2()
            } // H

            val w = Array(this@SBLSHSignatureGenerator.K) { v[it] }

            for (i in 0 until L) {
                for (j in 1..N) {
                    for (k in 1 until j) {
                        w[i * N + j - 1] = w[i * N + j - 1] - (w[i * N + k - 1] * w[i * N + j - 1].dot(v[i * N + k - 1])) // order of dot product matters for complex vectors!
                    }
                    w[i * N + j - 1] = w[i * N + j - 1] / w[i * N + j - 1].norm2()
                }
            }

            this.hyperplanes = w // H ̃
        }

        /**
         * Compute the signature of a vector.
         *
         * @param vector
         * @return The signature.
         */
        fun signature(vector: VectorValue<*>): BooleanArray {
            val signature = BooleanArray(this.hyperplanes.size)
            for (i in this.hyperplanes.indices) {
                signature[i] = this.hyperplanes[i].dot(vector).asInt().value >= 0
            }
            return signature
        }

        /**
         * Compute the signature of a complex vector. Take every even hyperplane for the real part, every odd one for the imaginary part.
         *
         * todo: are there better ways to incorporate the imaginary part?
         *       if in the end the distance measure is the absolute IP, could we maybe do something like this here?
         *
         * @param vector
         * @return The signature.
         */
        fun signatureComplex(vector: ComplexVectorValue<*>): BooleanArray {
            return BooleanArray(this.hyperplanes.size) {
                if (it % 2 == 0) {
                    this.hyperplanes[it].dot(vector).real.asInt().value >= 0
                } else {
                    this.hyperplanes[it].dot(vector).imaginary.asInt().value >= 0
                }
            }
        }
    }
}