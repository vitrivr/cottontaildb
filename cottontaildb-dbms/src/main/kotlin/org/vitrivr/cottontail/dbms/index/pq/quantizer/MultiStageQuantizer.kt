package org.vitrivr.cottontail.dbms.index.pq.quantizer

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.signature.IVFPQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.utilities.math.clustering.KMeansClusterer

/**
 * A Product Quantizer (PQ) can be used to quantize a [VectorValue]
 **
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class MultiStageQuantizer(val coarse: PQCodebook, val fine: Array<PQCodebook>) {

    companion object {
        /**
         * Generates a new [MultiStageQuantizer] instance for the given [IVFPQIndexConfig] and training data.
         *
         * @param distance The [VectorDistance] to create the [MultiStageQuantizer] for.
         * @param data A list of [VectorValue]s to train the new [MultiStageQuantizer] with.
         * @param config The [IVFPQIndexConfig]
         * @return Newly learned [MultiStageQuantizer].
         */
        @Suppress("UNCHECKED_CAST")
        fun learnFromData(distance: VectorDistance<*>, data: List<VectorValue<*>>, config: IVFPQIndexConfig): MultiStageQuantizer {
            /* Determine logical size and perform some sanity checks. */
            val logicalSize = distance.type.logicalSize
            val subspaces = config.numberOfSubspaces(logicalSize)
            val dimensionsPerSubspace = logicalSize / subspaces

            require(data.all { it.logicalSize == logicalSize }) { "Training of product quantizer not possible; dimensionality of training data and distance function don't match." }
            require(logicalSize >= subspaces) { "Training of product quantizer not possible; logical size of data must be greater or equal to number of subspaces." }
            require(dimensionsPerSubspace * subspaces == logicalSize) { "Training of product quantizer not possible; vector size of $logicalSize does not allow for equally spaced subspaces." }

            /* Prepare k-means clusterer. */
            val reshape = distance.copy(dimensionsPerSubspace)
            val coarseClusterer = KMeansClusterer(config.numCoarseCentroids, distance, JDKRandomGenerator(config.seed))
            val fineClusterer = KMeansClusterer(config.numCentroids, reshape, JDKRandomGenerator(config.seed))

            /* Prepare codebooks. */
            val coarse = PQCodebook(distance, coarseClusterer.cluster(data).map { it.center }.toTypedArray())
            val fine = Array(subspaces) { j ->
                val subspaceData = data.map { v -> v.slice(j * dimensionsPerSubspace, dimensionsPerSubspace) }
                val clusters = fineClusterer.cluster(subspaceData).map { it.center }.toTypedArray()
                PQCodebook(reshape, clusters)
            }

            /* Return quantizer. */
            return MultiStageQuantizer(coarse, fine)
        }
    }


    /** The number of subspaces as defined in this [MultiStageQuantizer] implementation. */
    private val numberOfSubspaces
        get() = this.fine.size

    /**
     * Quantizes the specified [VectorValue] into a [IVFPQSignature], which is simply a concatenation
     * of the representative centroid in each subspace for the specified vector
     *
     * @param vector The [VectorValue] to calculate the [IVFPQSignature] for.
     * @return The calculated [IVFPQSignature]
     */
    fun quantize(tupleId: TupleId, vector: VectorValue<*>): Pair<Short,IVFPQSignature> {
        val bucket = this.coarse.quantize(vector).toShort()
        val signature = IVFPQSignature(tupleId, ShortArray(this.numberOfSubspaces) { j ->
            val codebook = this.fine[j]
            codebook.quantize(vector.slice((j) * codebook.subspaceSize, codebook.subspaceSize)).toShort()
        })
        return bucket to signature
    }

    /**
     * Generates and returns a [PQLookupTable] for the given query [VectorValue] and [VectorDistance].
     *
     * @param query The [VectorValue] to generate the [PQLookupTable] for.
     * @return The [PQLookupTable] for the given [VectorDistance].
     */
    fun createLookupTable(query: VectorValue<*>): PQLookupTable = when (val value = this.fine.first().distance) {
        is ManhattanDistance<*> -> PQLookupTable.Manhattan(query, this.fine)
        is EuclideanDistance<*> -> PQLookupTable.Euclidean(query, this.fine)
        is SquaredEuclideanDistance<*> -> PQLookupTable.SquaredEuclidean(query, this.fine)
        else -> throw IllegalStateException("The distance function ${value.signature} us not supported for product quantization.")
    }

    /**
     * Converts this [MultiStageQuantizer] to a [SerializableMultiStageProductQuantizer].
     *
     * @return [SerializableMultiStageProductQuantizer]
     */
    fun toSerializableProductQuantizer(): SerializableMultiStageProductQuantizer = SerializableMultiStageProductQuantizer(
        Array(this.coarse.numberOfCentroids) {j ->
            DoubleArray(this.coarse.subspaceSize) { k ->
                this.coarse.centroids[j][k].value.toDouble()
            }
        },
        Array(this.numberOfSubspaces){ i ->
            Array(this.fine[i].numberOfCentroids) { j->
                DoubleArray(this.fine[i].subspaceSize) { k ->
                    this.fine[i].centroids[j][k].value.toDouble()
                }
            }
        }
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MultiStageQuantizer) return false

        if (coarse != other.coarse) return false
        if (!fine.contentEquals(other.fine)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = coarse.hashCode()
        result = 31 * result + fine.contentHashCode()
        return result
    }
}