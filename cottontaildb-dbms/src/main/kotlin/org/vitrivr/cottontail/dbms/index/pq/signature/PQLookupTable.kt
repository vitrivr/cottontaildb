package org.vitrivr.cottontail.dbms.index.pq.signature

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.InnerProductDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.quantizer.PQCodebook
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A lookup table like data structure used by [PQIndex] to obtain approximate distances from [SPQSignature]s using the ADC algorithm outlined in [1].
 *
 * References:
 * [1] Jegou, Herve, et al. "Product Quantization for Nearest Neighbor Search." IEEE Transactions on Pattern Analysis and Machine Intelligence. 2010.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.3.0
 */
sealed interface PQLookupTable {
    /** The ADC lookup table. */
    val data: Array<DoubleArray>

    /**
     * Calculates and returns the approximate distance for the given [SPQSignature].
     *
     * @param signature The [PQSignature] to calculate the distance for.
     * @return Approximate distance.
     */
    fun approximateDistance(signature: PQSignature): Double

    /**
     * A [PQLookupTable] implementation for the [ManhattanDistance].
     *
     * @param query The [VectorValue] to prepare [PQLookupTable] for.
     * @param codebooks An [Array] of [PQCodebook]s.
     * @return [PQLookupTable]
     */
    class Manhattan(query: VectorValue<*>, codebooks: Array<PQCodebook>): PQLookupTable {
        override val data = Array(codebooks.size) { i ->
            val codebook = codebooks[i]
            val subspaceQuery = query.slice(i * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { code -> codebook.distanceFrom(subspaceQuery, code) }
        }
        override fun approximateDistance(signature: PQSignature): Double {
            var sum = 0.0
            for ((i,c) in signature.cells.withIndex()) {
                sum += this.data[i][c.toInt()]
            }
            return sum
        }
    }

    /**
     * A [PQLookupTable] implementation for the [EuclideanDistance].
     *
     * @param query The [VectorValue] to prepare [PQLookupTable] for.
     * @param codebooks An [Array] of [PQCodebook]s.
     * @return [PQLookupTable]
     */
    class Euclidean(query: VectorValue<*>, codebooks: Array<PQCodebook>): PQLookupTable {
        override val data = Array(codebooks.size) { i ->
            val codebook = codebooks[i]
            val subspaceQuery = query.slice(i * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { code -> codebook.distanceFrom(subspaceQuery, code).pow(2) }
        }
        override fun approximateDistance(signature: PQSignature): Double {
            var sum = 0.0
            for ((i,c) in signature.cells.withIndex()) {
                sum += this.data[i][c.toInt()]
            }
            return sqrt(sum)
        }
    }

    /**
     * A [PQLookupTable] implementation for the [CosineDistance].
     *
     * Warning: This is a rather heuristical method, since [CosineDistance] cannot be calculated in parts.
     *
     * @param query The [VectorValue] to prepare [PQLookupTable] for.
     * @param codebooks An [Array] of [PQCodebook]s.
     * @return [PQLookupTable]
     */
    class Cosine(query: VectorValue<*>, codebooks: Array<PQCodebook>): PQLookupTable {
        override val data = Array(codebooks.size) { i ->
            val codebook = codebooks[i]
            val subspaceQuery = query.slice(i * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { code -> codebook.distanceFrom(subspaceQuery, code).pow(2) }
        }
        override fun approximateDistance(signature: PQSignature): Double {
            var sum = 0.0
            for ((i,c) in signature.cells.withIndex()) {
                sum += this.data[i][c.toInt()]
            }
            return sqrt(sum)
        }
    }

    /**
     * A [PQLookupTable] implementation for the [InnerProductDistance].
     *
     * @param query The [VectorValue] to prepare [PQLookupTable] for.
     * @param codebooks An [Array] of [PQCodebook]s.
     * @return [PQLookupTable]
     */
    class InnerProduct(query: VectorValue<*>, codebooks: Array<PQCodebook>): PQLookupTable {
        override val data = Array(codebooks.size) { i ->
            val codebook = codebooks[i]
            val subspaceQuery = query.slice(i * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { code -> codebook.distanceFrom(subspaceQuery, code) }
        }
        override fun approximateDistance(signature: PQSignature): Double {
            var sum = 0.0
            for ((i,c) in signature.cells.withIndex()) {
                sum += this.data[i][c.toInt()]
            }
            return sum
        }
    }

    /**
     * A [PQLookupTable] implementation for the [SquaredEuclidean].
     *
     * @param query The [VectorValue] to prepare [PQLookupTable] for.
     * @param codebooks An [Array] of [PQCodebook]s.
     * @return [PQLookupTable]
     */
    class SquaredEuclidean(query: VectorValue<*>, codebooks: Array<PQCodebook>): PQLookupTable {
        override val data = Array(codebooks.size) { i ->
            val codebook = codebooks[i]
            val subspaceQuery = query.slice(i * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { code -> codebook.distanceFrom(subspaceQuery, code).pow(2) }
        }
        override fun approximateDistance(signature: PQSignature): Double {
            var sum = 0.0
            for ((i,c) in signature.cells.withIndex()) {
                sum += this.data[i][c.toInt()]
            }
            return sum
        }
    }
}