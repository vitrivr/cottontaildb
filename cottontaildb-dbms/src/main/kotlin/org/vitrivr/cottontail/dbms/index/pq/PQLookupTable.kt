package org.vitrivr.cottontail.dbms.index.pq

/**
 * A lookup table like data structure used by [PQIndex] to obtain approximate distances from [PQSignature]s
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.1
 */
@JvmInline
value class PQLookupTable(val data: Array<DoubleArray>) {
    /**
     * Calculates and returns the approximate distance for the given [PQSignature].
     *
     * @param signature The [PQSignature] to calculate the distance for.
     * @return Approximate distance.
     */
    fun approximateDistance(signature: PQSignature): Double {
        var distance = 0.0
        for ((i, d) in this.data.withIndex()) {
            distance += d[signature.cells[i]]
        }
        return distance
    }
}