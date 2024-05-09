package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.squaredeuclidean.SquaredEuclideanDistance
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import java.io.ByteArrayInputStream

/**
 * Configuration claess for [IVFPQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class IVFPQIndexConfig(val distance: Name.FunctionName, val numCoarseCentroids: Int, val numCentroids: Int, val numSubspace: Int, val seed: Int = System.currentTimeMillis().toInt()) : IndexConfig<IVFPQIndex> {

    companion object {
        /** The maximum number of subspaces. We cap this at 32 to limit the code length.  */
        private const val MAXIMUM_NUMBER_OF_SUBSPACES = 32

        /** Default value for the number of centroids as recommended by Jegou et al. */
        const val DEFAULT_CENTROIDS = 512

        /** Default value for the number of centroids as recommended by Jegou et al. */
        const val DEFAULT_SUBSPACES = 8

        /** DEfault value for the number of centroids as recommended by Jegou et al. */
        const val DEFAULT_COARSE_CENTROIDS = 1024

        /** Configuration key for the number of subspaces. */
        const val KEY_DISTANCE = "ivfpq.distance"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_CENTROIDS = "ivfpq.centroids"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_COARSE_CENTROIDS = "ivfpq.coarse_centroids"

        /** Configuration key for the number of centroids. */
        const val KEY_SEED = "ivfpq.seed"

        /** Set of supported distances. */
        val SUPPORTED_DISTANCES: Set<Name.FunctionName> = setOf(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, SquaredEuclideanDistance.FUNCTION_NAME, CosineDistance.FUNCTION_NAME)
    }

    /**
     * [ComparableBinding] for [IVFPQIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream) = IVFPQIndexConfig(
            Name.FunctionName.create(StringBinding.BINDING.readObject(stream)),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<IVFPQIndexConfig>) {
            require(`object` is IVFPQIndexConfig) { "IVFPQIndexConfig.Binding can only be used to serialize instances of IVFPQIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.numCoarseCentroids)
            IntegerBinding.writeCompressed(output, `object`.numCentroids)
            IntegerBinding.writeCompressed(output, `object`.numSubspace)
            IntegerBinding.writeCompressed(output, `object`.seed)
        }
    }

    init {
        /* Range of sanity checks. */
        require(this.numCentroids > 0) { "IVFPQIndex requires at least one centroid." }
        require(this.numCoarseCentroids > 0) { "IVFPQIndex requires at least one centroid." }
        require(this.numCentroids <= Short.MAX_VALUE) { "IVFPQIndex supports a maximum number of ${Short.MAX_VALUE} coarse centroids." }
        require(this.numCoarseCentroids <= Short.MAX_VALUE) { "IVFPQIndex supports a maximum number of ${Short.MAX_VALUE} coarse centroids." }
        require(this.distance in SUPPORTED_DISTANCES) { "PQIndex only support L1, L2, L2SQUARED, COSINE and INNERPRODUCT distance."}
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
        var subspaces = this.numSubspace
        do {
            if (d % subspaces == 0) return subspaces
        } while ((++subspaces) <= MAXIMUM_NUMBER_OF_SUBSPACES)

        /* We have to try lower; which will increase distance distortion. */
        subspaces = this.numSubspace
        while (subspaces-- > 1) {
            if (d % subspaces == 0) return subspaces
        }
        return 1
    }

    /**
     * Converts this [PQIndexConfig] to a [Map] of key-value pairs.
     *
     * @return [Map]
     */
    override fun toMap(): Map<String, String> = mapOf(
        KEY_DISTANCE to this.distance.simple,
        KEY_NUM_CENTROIDS to this.numCentroids.toString(),
        KEY_NUM_COARSE_CENTROIDS to this.numCoarseCentroids.toString(),
        KEY_SEED to this.seed.toString(),
    )
}