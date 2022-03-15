package org.vitrivr.cottontail.dbms.index.pq

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.clustering.KMeansClusterer

/**
 * Product Quantizer (PQ) that minimizes inner product error. Input data should be permuted for better results!
 *
 * Roughly following Guo et al. 2015 - Quantization based Fast Inner Product Search
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.0.0
 */
class ProductQuantizer private constructor(private val codebooks: Array<PQCodebook>) {

    companion object {
        /**
         * Generates a new [ProductQuantizer] instance for the given [PQIndexConfig]and training data.
         *
         * @param distance The [VectorDistance] to create the [ProductQuantizer] for.
         * @param data A list of [VectorValue]s to train the new [ProductQuantizer] with.
         * @param config The [PQIndexConfig]
         * @return Newly learned [ProductQuantizer].
         */
        @Suppress("UNCHECKED_CAST")
        fun learnFromData(distance: VectorDistance<*>, data: List<VectorValue<*>>, config: PQIndexConfig): ProductQuantizer {
            /* Determine logical size and perform some sanity checks. */
            val logicalSize = distance.type.logicalSize
            val numSubspaces = config.numSubspaces ?: defaultNumberOfSubspaces(logicalSize)
            val dimensionsPerSubspace = logicalSize / numSubspaces

            require(data.all { it.logicalSize == logicalSize }) { "Training of product quantizer not possible; dimensionality of training data and distance function don't match." }
            require(logicalSize >= numSubspaces) { "Training of product quantizer not possible; logical size of data must be greater or equal to number of subspaces." }
            require(dimensionsPerSubspace * numSubspaces == logicalSize) { "Training of product quantizer not possible; vector size of $logicalSize does not allow for equally spaced subspaces." }

            /* Prepare k-means clusterer. */
            val reshape = distance.copy(dimensionsPerSubspace)
            val clusterer = KMeansClusterer(config.numCentroids, reshape, JDKRandomGenerator(config.seed.toInt()))

            /* Prepare codebooks. */
            val codebooks = Array(numSubspaces) {
                val subspaceData = data.map { v -> v.slice(it * dimensionsPerSubspace, dimensionsPerSubspace) }
                PQCodebook(reshape, clusterer.cluster(subspaceData).map { it.center }.toTypedArray())
            }

            /* Return quantizer. */
            return ProductQuantizer(codebooks)
        }

        /**
         * Reconstructs a  [ProductQuantizer] a [VectorDistance], a given number of subspaces and a list of [DoubleArray] centroid vectors.
         *
         * @param distance The [VectorDistance] to create the [ProductQuantizer] for.
         * @param config The [PQIndexConfig]
         * @return Reconstructed [ProductQuantizer]
         */
        fun loadFromConfig(distance: VectorDistance<*>, config: PQIndexConfig): ProductQuantizer {
            /* Determine logical size and perform some sanity checks. */
            val logicalSize = distance.type.logicalSize
            val numSubspaces = config.numSubspaces ?: defaultNumberOfSubspaces(logicalSize)
            val dimensionsPerSubspace = logicalSize / numSubspaces

            val reshaped = distance.copy(dimensionsPerSubspace)
            val codebooks = Array(numSubspaces) { _ ->
                PQCodebook(reshaped, Array(config.centroids.size) { j ->
                    require(dimensionsPerSubspace == config.centroids[j].size) { "Reconstruction of product quantizer not possible; dimension per subspace doesn't match with size of stored centroids."}
                    when(distance.type) {
                        is Types.DoubleVector -> DoubleVectorValue(config.centroids[j])
                        is Types.FloatVector -> FloatVectorValue(config.centroids[j])
                        is Types.LongVector -> LongVectorValue(config.centroids[j].toList())
                        is Types.IntVector -> IntVectorValue(config.centroids[j].toList())
                        else -> throw IllegalArgumentException("Reconstruction of product quantizer not possible; type ${distance.type} not supported.")
                    }
                })
            }
            return ProductQuantizer(codebooks)
        }

        /**
         * Dynamically determines the number of subspaces for the given dimension.
         *
         * @param d The dimensionality of the vector.
         * @return Number of subspaces to use.
         */
        fun defaultNumberOfSubspaces(d: Int): Int {
            val start: Int = when {
                d == 1 -> 1
                d == 2 -> 2
                d <= 8 -> 4
                d <= 64 -> 4
                d <= 256 -> 8
                d <= 1024 -> 16
                d <= 4096 -> 32
                else -> 64
            }
            var subspaces = start
            while (subspaces < d && subspaces < Byte.MAX_VALUE) {
                if (d % subspaces == 0) {
                    return subspaces
                }
                subspaces++
            }
            return start
        }
    }

    /** The number of subspaces as defined in this [ProductQuantizer] implementation. */
    private val numberOfSubspaces
        get() = this.codebooks.size

    /**
     * Quantizes the specified [VectorValue] into a [PQSignature], which is simply a concatenation
     * of the representative centroid in each subspace for the specified vector
     *
     * @param v The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun quantize(v: VectorValue<*>): PQSignature {
        return PQSignature(IntArray(this.numberOfSubspaces) {
            val codebook = this.codebooks[it]
            val subvector = v.slice(it * codebook.subspaceSize, codebook.subspaceSize)
            codebook.quantize(subvector)
        })
    }

    /**
     * Generates and returns a [PQLookupTable] for the given query [VectorValue] and [VectorDistance].
     *
     * @param query The [VectorValue] to generate the [PQLookupTable] for.
     * @return The [PQLookupTable] for the given [VectorDistance].
     */
    fun createLookupTable(query: VectorValue<*>): PQLookupTable = PQLookupTable(
        Array(this.numberOfSubspaces) { k ->
            val codebook = this.codebooks[k]
            val subspaceQuery = query.slice(k * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { codebook.distance(subspaceQuery, codebook.centroids[it])!!.value }
        }
    )

    /**
     * Dumps the centroids that make up this [ProductQuantizer] into a list of [DoubleArray]s.
     *
     * @return List of [DoubleArray] of all centroids that make-up this [ProductQuantizer].
     */
    fun centroids(): List<DoubleArray> = this.codebooks.flatMap { cb ->
        cb.centroids.map { v -> DoubleArray(v.logicalSize) { v[it].value.toDouble()} }
    }

    /**
     * A codebook that can be used to quantize a [VectorValue] (or more precisely, a subspace thereof)
     * to a learned centroid. Used for product quantization based index structures.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    private class PQCodebook(val distance: VectorDistance<*>, val centroids: Array<VectorValue<*>>) {

        init {
            require(this.centroids.all { it.logicalSize == this.distance.type.logicalSize }) { "Dimensionality of centroids and distance function do not match for PQ codebook." }
        }

        /** The size of the subspace encoded by this [PQCodebook]. */
        val subspaceSize: Int
            get() = this.centroids[0].logicalSize

        /** The number of centroids contained in this [PQCodebook]. */
        val numberOfCentroids: Int
            get() = this.centroids.size

        /**
         * Quantizes the given subvector [VectorValue] and returns the index of the centroid it belongs to.
         *
         * @param subvector The subvector [VectorValue] to quantize.
         * @return The index of the centroid the given [VectorValue] belongs to.
         */
        fun quantize(subvector: VectorValue<*>): Int {
            var mahIndex = 0
            var mah = Double.POSITIVE_INFINITY
            for ((i, c) in this.centroids.withIndex()) {
                val dist = this.distance(c, subvector)!!.value
                if (dist < mah) {
                    mah = dist
                    mahIndex = i
                }
            }
            return mahIndex
        }
    }
}
