package org.vitrivr.cottontail.database.index.pq

/**
 * Data structure to hold inner products between a query vector and all centroids for
 * all subspaces.
 * This is intended as a lookup table for running PQ based queries
 */
inline class PQCentroidQueryIPFloat(val data: Array<FloatArray>) {
    inline fun approximateIP(signature: IntArray): Float {
        var ip = 0.0F
        for (i in signature.indices) {
            ip += data[i][signature[i]]
        }
        return ip
    }

    /**
     * for a larger array signature with an offset
     */
    inline fun approximateIP(signature: IntArray, start: Int, length: Int): Float {
        var ip = 0.0F
        for (i in 0 until length) {
            ip += data[i][signature[i + start]]
        }
        return ip
    }

    /**
     * for a larger array signature with an offset
     */
    @ExperimentalUnsignedTypes
    inline fun approximateIP(signature: UShortArray, start: Int, length: Int): Float {
        var ip = 0.0F
        for (i in 0 until length) {
            ip += data[i][signature[i + start].toInt()]
        }
        return ip
    }

    /**
     * for a larger array signature with an offset
     */
    @ExperimentalUnsignedTypes
    inline fun approximateIP(signature: UByteArray, start: Int, length: Int): Float {
        var ip = 0.0F
        for (i in 0 until length) {
            ip += data[i][signature[i + start].toInt()]
        }
        return ip
    }
}