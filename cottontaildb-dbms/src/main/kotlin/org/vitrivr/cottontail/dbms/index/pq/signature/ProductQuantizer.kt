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
 * @version 2.0.1
 */
data class ProductQuantizer constructor(val codebooks: Array<PQCodebook>) {

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
            val subspaces = config.numberOfSubspaces(logicalSize)
            val dimensionsPerSubspace = logicalSize / subspaces

            require(data.all { it.logicalSize == logicalSize }) { "Training of product quantizer not possible; dimensionality of training data and distance function don't match." }
            require(logicalSize >= subspaces) { "Training of product quantizer not possible; logical size of data must be greater or equal to number of subspaces." }
            require(dimensionsPerSubspace * subspaces == logicalSize) { "Training of product quantizer not possible; vector size of $logicalSize does not allow for equally spaced subspaces." }

            /* Prepare k-means clusterer. */
            val reshape = distance.copy(dimensionsPerSubspace)
            val clusterer = KMeansClusterer(config.numCentroids, reshape, JDKRandomGenerator(config.seed))

            /* Prepare codebooks. */
            val codebooks = Array(subspaces) { j ->
                val subspaceData = data.map { v -> v.slice(j * dimensionsPerSubspace, dimensionsPerSubspace) }
                val clusters = clusterer.cluster(subspaceData).map { it.center }.toTypedArray()
                PQCodebook(reshape, clusters)
            }

            /* Return quantizer. */
            return ProductQuantizer(codebooks)
        }
    }

    /** The number of subspaces as defined in this [ProductQuantizer] implementation. */
    private val numberOfSubspaces
        get() = this.codebooks.size

    /**
     * Quantizes the specified [VectorValue] into a [PQSignature], which is simply a concatenation
     * of the representative centroid in each subspace for the specified vector
     *
     * @param vector The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun quantize(vector: VectorValue<*>): PQSignature {
        val ret = PQSignature(ShortArray(this.numberOfSubspaces) { j ->
            val codebook = this.codebooks[j]
            codebook.quantize(vector.slice(j * codebook.subspaceSize, codebook.subspaceSize)).toShort()
        })
        return ret
    }

    /**
     * Generates and returns a [PQLookupTable] for the given query [VectorValue] and [VectorDistance].
     *
     * @param query The [VectorValue] to generate the [PQLookupTable] for.
     * @return The [PQLookupTable] for the given [VectorDistance].
     */
    fun createLookupTable(query: VectorValue<*>): PQLookupTable = when (val value = this.codebooks.first().distance) {
        is ManhattanDistance<*> -> PQLookupTable.Manhattan(query, this.codebooks)
        is EuclideanDistance<*> -> PQLookupTable.Euclidean(query, this.codebooks)
        is SquaredEuclideanDistance<*> -> PQLookupTable.SquaredEuclidean(query, this.codebooks)
        else -> throw IllegalStateException("The distance function ${value.signature} us not supported for product quantization.")
    }

    /**
     * Converts this [ProductQuantizer] to a [SerializableProductQuantizer].
     *
     * @return [SerializableProductQuantizer]
     */
    fun toSerializableProductQuantizer(): SerializableProductQuantizer = SerializableProductQuantizer(Array(this.numberOfSubspaces){ i ->
        Array(this.codebooks[i].numberOfCentroids) { j->
            DoubleArray(this.codebooks[i].subspaceSize) { k ->
                this.codebooks[i].centroids[j][k].value.toDouble()
            }
        }
    })

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ProductQuantizer) return false
        if (!this.codebooks.contentEquals(other.codebooks)) return false
        return true
    }

    override fun hashCode(): Int {
        return codebooks.contentHashCode()
    }

    /**
     * A codebook that can be used to quantize a [VectorValue] (or more precisely, a subspace thereof)
     * to a learned centroid. Used for product quantization based index structures.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    data class PQCodebook(val distance: VectorDistance<*>, val centroids: Array<VectorValue<*>>) {

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

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is PQCodebook) return false
            if (this.distance.signature != other.distance.signature) return false
            if (this.centroids.size != other.centroids.size) return false
            for (i in 0 until this.centroids.size) {
                for (j in 0 until this.centroids[i].logicalSize) {
                    if (this.centroids[i][j] != other.centroids[i][j]) return false
                }
            }
            return true
        }

        override fun hashCode(): Int {
            var result = this.distance.signature.hashCode()
            result = 31 * result + this.centroids.contentHashCode()
            return result
        }
    }
}
