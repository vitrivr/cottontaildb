package org.vitrivr.cottontail.database.index.pq

/**
 * A lookup table like data structure used by [PQIndex] to obtain approximate distances from [PQShortSignature]s
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
inline class PQLookupTable(val data: Array<DoubleArray>) {
    /**
     * Calculates and returns the approximate distance for the given [PQShortSignature].
     *
     * @param signature The [PQShortSignature] to calculate the distance for.
     * @return Approximate distance.
     */
    fun approximateDistance(signature: PQSignature): Double {
        var distance = 0.0
        for (i in signature.cells.indices) {
            distance += this.data[i][signature.cells[i]]
        }
        return distance
    }
}