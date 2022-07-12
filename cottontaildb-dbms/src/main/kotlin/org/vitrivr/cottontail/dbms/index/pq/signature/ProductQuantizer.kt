package org.vitrivr.cottontail.dbms.index.pq.signature

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.utilities.math.clustering.KMeansClusterer

/**
 * Product Quantizer (PQ) that minimizes inner product error. Input data should be permuted for better results!
 *
 * Roughly following Guo et al. 2015 - Quantization based Fast Inner Product Search
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.0.0
 */
class ProductQuantizer constructor(val codebooks: Array<PQCodebook>) {

    companion object {
        /** Recommended number of subspaces according to [1]. */
        private const val RECOMMENDED_NUMBER_OF_SUBSPACES = 8

        /** The maximum number of subspaces. We cap this at 32 to limit the code length.  */
        private const val MAXIMUM_NUMBER_OF_SUBSPACES = 32

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
            val numSubspaces = numberOfSubspaces(logicalSize)
            val dimensionsPerSubspace = logicalSize / numSubspaces

            require(data.all { it.logicalSize == logicalSize }) { "Training of product quantizer not possible; dimensionality of training data and distance function don't match." }
            require(logicalSize >= numSubspaces) { "Training of product quantizer not possible; logical size of data must be greater or equal to number of subspaces." }
            require(dimensionsPerSubspace * numSubspaces == logicalSize) { "Training of product quantizer not possible; vector size of $logicalSize does not allow for equally spaced subspaces." }

            /* Prepare k-means clusterer. */
            val reshape = distance.copy(dimensionsPerSubspace)
            val clusterer = KMeansClusterer(config.numCentroids, reshape, JDKRandomGenerator(config.seed))

            /* Prepare codebooks. */
            val codebooks = Array(numSubspaces) { j ->
                val subspaceData = data.map { v -> v.slice(j * dimensionsPerSubspace, dimensionsPerSubspace) }
                val clusters = clusterer.cluster(subspaceData).map { it.center }.toTypedArray()
                PQCodebook(reshape, clusters)
            }

            /* Return quantizer. */
            return ProductQuantizer(codebooks)
        }

        /**
         * Determines the number of subspaces for the given vector dimension (size).
         *
         * This method uses the recommendations given in [1] and starts with 8 subspaces and tries to find an adequate number given the vector's dimensionality.
         *
         * @param d The dimensionality of the vector.
         * @return Number of subspaces to use.
         */
        fun numberOfSubspaces(d: Int): Int {
            var subspaces = RECOMMENDED_NUMBER_OF_SUBSPACES
            while (subspaces++ <= MAXIMUM_NUMBER_OF_SUBSPACES) {
                if (d % subspaces == 0) return subspaces
            }
            /* We have to try lower; which will increase distance distortion. */
            subspaces= RECOMMENDED_NUMBER_OF_SUBSPACES
            while (subspaces-- > 1) {
                if (d % subspaces == 0) return subspaces
            }
            return 1
        }
    }

    /** The number of subspaces as defined in this [ProductQuantizer] implementation. */
    private val numberOfSubspaces = this.codebooks.size

    /**
     * Quantizes the specified [VectorValue] into a [PQSignature], which is simply a concatenation
     * of the representative centroid in each subspace for the specified vector
     *
     * @param vector The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun quantize(vector: VectorValue<*>): PQSignature {
        val ret = PQSignature(IntArray(this.numberOfSubspaces) { j ->
            val codebook = this.codebooks[j]
            codebook.quantize(vector.slice(j * codebook.subspaceSize, codebook.subspaceSize))
        })
        return ret
    }

    /**
     * Generates and returns a [PQLookupTable] for the given query [VectorValue] and [VectorDistance].
     *
     * @param query The [VectorValue] to generate the [PQLookupTable] for.
     * @return The [PQLookupTable] for the given [VectorDistance].
     */
    fun createLookupTable(query: VectorValue<*>): PQLookupTable {
        val lat = Array(this.numberOfSubspaces) { j ->
            val codebook = this.codebooks[j]
            val subspaceQuery = query.slice(j * codebook.subspaceSize, codebook.subspaceSize)
            DoubleArray(codebook.numberOfCentroids) { code -> codebook.distanceFrom(subspaceQuery, code) }
        }
        return when (this.codebooks.first().distance) {
            is ManhattanDistance<*> -> PQLookupTable.Manhattan(lat)
            is EuclideanDistance<*> -> PQLookupTable.Euclidean(lat)
            is SquaredEuclideanDistance<*> -> PQLookupTable.SquaredEuclidean(lat)
            else -> throw IllegalStateException("")
        }
    }

    /**
     * Converts this [ProductQuantizer] to a [SerializableProductQuantizer].
     *
     * @return [SerializableProductQuantizer]
     */
    fun toSerializableProductQuantizer(): SerializableProductQuantizer = SerializableProductQuantizer(Array(this.codebooks.size){ i ->
        Array(this.codebooks[0].numberOfCentroids) { j->
            DoubleArray(this.codebooks[0].subspaceSize) { k ->
                this.codebooks[i].centroids[j][k].value.toDouble()
            }
        }
    })

    /**
     * A codebook that can be used to quantize a [VectorValue] (or more precisely, a subspace thereof)
     * to a learned centroid. Used for product quantization based index structures.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    class PQCodebook(val distance: VectorDistance<*>, val centroids: Array<VectorValue<*>>) {

        init {
            require(this.centroids.all { it.logicalSize == this.distance.type.logicalSize }) { "Dimensionality of centroids and distance function do not match for PQ codebook." }
        }

        /** The size of the subspace encoded by this [PQCodebook]. */
        val subspaceSize: Int = this.centroids[0].logicalSize

        /** The number of centroids contained in this [PQCodebook]. */
        val numberOfCentroids: Int = this.centroids.size

        /**
         * Quantizes the given subvector [VectorValue] and returns the index of the centroid it belongs to.
         *
         * @param subvector The subvector [VectorValue] to quantize.
         * @return The index of the centroid the given [VectorValue] belongs to.
         */
        fun quantize(subvector: VectorValue<*>): Int {
            var smallestIndexSeen = 0
            var smallestDistSeen = this.distance(subvector, this.centroids[0])!!
            for (i in 1 until this.centroids.size) {
                val newDist = this.distance(subvector, this.centroids[i])
                if (newDist!!.value < smallestDistSeen.value) {
                    smallestDistSeen = newDist
                    smallestIndexSeen = i
                }
            }
            return smallestIndexSeen
        }

        /**
         * Calculates the distance of the given [VectorValue] from the centroid with the given index.
         *
         * @param subvector The [VectorValue] to evalute.
         * @param centroidIndex The centroid index.
         */
        fun distanceFrom(subvector: VectorValue<*>, centroidIndex: Int): Double {
            require(subvector.logicalSize == this.subspaceSize) { "Dimension mismatch between sub-vector and codebook." }
            return this.distance(subvector, this.centroids[centroidIndex])!!.value
        }
    }
}
