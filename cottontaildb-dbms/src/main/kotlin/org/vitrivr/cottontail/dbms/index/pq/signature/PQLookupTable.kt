package org.vitrivr.cottontail.dbms.index.pq.signature

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import kotlin.math.pow

/**
 * A lookup table like data structure used by [PQIndex] to obtain approximate distances from [PQSignature]s
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.1
 */
sealed class PQLookupTable(val data: Array<DoubleArray>) {

    /**
     * Calculates and returns the approximate distance for the given [PQSignature].
     *
     * @param signature The [PQSignature] to calculate the distance for.
     * @return Approximate distance.
     */
    abstract fun approximateDistance(signature: PQSignature): Double

    /**
     * A [PQLookupTable] implementation for the [ManhattanDistance].
     *
     * @param data The lookup-table data points.
     * @return [PQLookupTable]
     */
    class Manhattan(data: Array<DoubleArray>): PQLookupTable(data) {
        override fun approximateDistance(signature: PQSignature): Double
            = signature.cells.mapIndexed { i, c -> this.data[i][c] }.sum()
    }

    /**
     * A [PQLookupTable] implementation for the [EuclideanDistance].
     *
     * @param data The lookup-table data points.
     * @return [PQLookupTable]
     */
    class Euclidean(data: Array<DoubleArray>): PQLookupTable(data) {
        override fun approximateDistance(signature: PQSignature): Double
            = kotlin.math.sqrt(signature.cells.mapIndexed { i, c -> this.data[i][c].pow(2) }.sum())
    }

    /**
     * A [PQLookupTable] implementation for the [SquaredEuclidean].
     *
     * @param data The lookup-table data points.
     * @return [PQLookupTable]
     */
    class SquaredEuclidean(data: Array<DoubleArray>): PQLookupTable(data) {
        override fun approximateDistance(signature: PQSignature): Double
            = signature.cells.mapIndexed { i, c -> this.data[i][c].pow(2) }.sum()
    }
}