package ch.unibas.dmi.dbis.cottontail.database.index.lsh

import java.io.Serializable

/**
 * LSH implementation relying on Super-Bit, to bin vectors s times (stages)
 * in b buckets (per stage), in a space with d dimensions. Input vectors
 * with a high cosine similarity have a high probability of falling in the
 * same bucket.
 *
 * @param stages    stages
 * @param buckets   buckets (per stage)
 * @param dimension dimension of data space
 * @param seed      random number generator seed (sing the same value will guarantee identical hashes across object
 *                  instantiations)
 *
 * This class is inspired by Thibault Debatty (https://github.com/tdebatty/java-LSH).
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class LSH(private var stages: Int, private var buckets: Int, dimension: Int, seed: Int) : Serializable {

    private var LARGE_PRIME: Long = 433494437
    private var superBit: SuperBit? = null

    init {
        val k = stages * buckets / 2 // code length
        val N: Int = computeSuperBitDepth(dimension, k)
        var L = k / N
        superBit = SuperBit(dimension, N, L, seed)
    }

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
    fun hash(vector: DoubleArray): IntArray {
        return hashSignature(superBit!!.signature(vector))
    }

    /**
     * Convert float[] to double[] vector, then call [hash].
     *
     * @param floatVector
     * @return [hash] of the converted float[].
     */
    fun hash(floatVector: FloatArray): IntArray? {
        val vector = DoubleArray(floatVector.size)
        for (i in floatVector.indices) {
            vector[i] = floatVector[i].toDouble()
        }
        return hash(vector)
    }

    /**
     * Hash a signature.
     * The signature is divided in s stages. Each stage is hashed to one of the b buckets.
     *
     * @param signature
     * @return A vector of s integers (between 0 and b - 1)
     */
    private fun hashSignature(signature: BooleanArray): IntArray {
        // create an accumulator for each stage
        val acc = LongArray(stages)
        for (i in 0 until stages) {
            acc[i] = 0
        }
        // number of rows per stage
        val rows = signature.size / stages
        for (i in signature.indices) {
            var j: Long = 0
            if (signature[i]) j = (i + 1) * LARGE_PRIME
            // current stage
            val k = (i / rows).coerceAtMost(stages - 1)
            acc[k] = (acc[k] + j) % Int.MAX_VALUE
        }
        val vector = IntArray(stages)
        for (i in 0 until stages) {
            vector[i] = acc[i].toInt() % buckets
        }
        return vector
    }

}