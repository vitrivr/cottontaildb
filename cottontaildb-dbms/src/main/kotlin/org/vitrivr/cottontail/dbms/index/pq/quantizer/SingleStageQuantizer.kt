package org.vitrivr.cottontail.dbms.index.pq.quantizer

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.utilities.math.clustering.KMeansClusterer
import java.util.*

/**
 * Single-stage Product Quantizer (PQ) that can be used to quantize vectors to a codebook given a certain [VectorDistance].
 * Can be used to implement the ADC algorithm described in [1].
 *
 * References:
 * [1] Jegou, Herve, et al. "Product Quantization for Nearest Neighbor Search." IEEE Transactions on Pattern Analysis and Machine Intelligence. 2010.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.1.0
 */
data class SingleStageQuantizer constructor(val codebooks: Array<PQCodebook>) {

    companion object {
        /**
         * Generates a new [SingleStageQuantizer] instance for the given [PQIndexConfig]and training data.
         *
         * @param distance The [VectorDistance] to create the [SingleStageQuantizer] for.
         * @param data A list of [VectorValue]s to train the new [SingleStageQuantizer] with.
         * @param config The [PQIndexConfig]
         * @return Newly learned [SingleStageQuantizer].
         */
        @Suppress("UNCHECKED_CAST")
        fun learnFromData(distance: VectorDistance<*>, data: List<VectorValue<*>>, config: PQIndexConfig): SingleStageQuantizer {
            /* Determine logical size and perform some sanity checks. */
            val logicalSize = distance.type.logicalSize
            val subspaces = config.numberOfSubspaces(logicalSize)
            val dimensionsPerSubspace = logicalSize / subspaces

            require(data.all { it.logicalSize == logicalSize }) { "Training of product quantizer not possible; dimensionality of training data and distance function don't match." }
            require(logicalSize >= subspaces) { "Training of product quantizer not possible; logical size of data must be greater or equal to number of subspaces." }
            require(dimensionsPerSubspace * subspaces == logicalSize) { "Training of product quantizer not possible; vector size of $logicalSize does not allow for equally spaced subspaces." }

            /* Prepare k-means clusterer. */
            val reshape = distance.copy(dimensionsPerSubspace)
            val random = SplittableRandom(System.currentTimeMillis())
            val clusterer = KMeansClusterer(config.numCentroids, reshape, random)

            /* Prepare codebooks. */
            val codebooks = Array(subspaces) { j ->
                val subspaceData = data.map { v -> v.slice(j * dimensionsPerSubspace, dimensionsPerSubspace) }
                val clusters = clusterer.cluster(subspaceData).map { it.center }.toTypedArray()
                PQCodebook(reshape, clusters)
            }

            /* Return quantizer. */
            return SingleStageQuantizer(codebooks)
        }
    }

    /** The number of subspaces as defined in this [SingleStageQuantizer] implementation. */
    private val numberOfSubspaces
        get() = this.codebooks.size

    /**
     * Quantizes the specified [VectorValue] into a [SPQSignature], which is simply a concatenation
     * of the representative centroid in each subspace for the specified vector
     *
     * @param vector The [VectorValue] to calculate the [SPQSignature] for.
     * @return The calculated [SPQSignature]
     */
    fun quantize(vector: VectorValue<*>): SPQSignature {
        val subspaces = ShortArray(this.numberOfSubspaces) {
            val codebook = this.codebooks[it]
            codebook.quantize(vector.slice(it * codebook.subspaceSize, codebook.subspaceSize)).toShort()
        }
        return SPQSignature(subspaces)
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
     * Converts this [SingleStageQuantizer] to a [SerializableSingleStageProductQuantizer].
     *
     * @return [SerializableSingleStageProductQuantizer]
     */
    fun toSerializableProductQuantizer(): SerializableSingleStageProductQuantizer = SerializableSingleStageProductQuantizer(Array(this.numberOfSubspaces){ i ->
        Array(this.codebooks[i].numberOfCentroids) { j->
            DoubleArray(this.codebooks[i].subspaceSize) { k ->
                this.codebooks[i].centroids[j][k].value.toDouble()
            }
        }
    })

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SingleStageQuantizer) return false
        if (!this.codebooks.contentEquals(other.codebooks)) return false
        return true
    }

    override fun hashCode(): Int {
        return codebooks.contentHashCode()
    }
}
