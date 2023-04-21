package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import java.io.ByteArrayInputStream

/**
 * Configuration class for [PQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.3.0
 */
data class PQIndexConfig(val distance: Name.FunctionName, val numCentroids: Int, val subspaces: Int, val seed: Int = System.currentTimeMillis().toInt()) : IndexConfig<PQIndex> {

    companion object {


        /** The maximum number of subspaces. We cap this at 32 to limit the code length.  */
        private const val MAXIMUM_NUMBER_OF_SUBSPACES = 32

        /** Configuration key for the number of subspaces. */
        const val KEY_DISTANCE = "pq.distance"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_SUBSPACES = "pq.subspaces"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_CENTROIDS = "pq.centroids"

        /** Configuration key for the number of centroids. */
        const val KEY_SEED = "pq.seed"

        /** Default value for the number of centroids as recommended by [1]. */
        const val DEFAULT_CENTROIDS = 256

        /** Recommended number of subspaces according to [1]. */
        const val DEFAULT_SUBSPACES = 8

        /** Set of supported distances. */
        val SUPPORTED_DISTANCES: Set<Name.FunctionName> = setOf(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, SquaredEuclideanDistance.FUNCTION_NAME, CosineDistance.FUNCTION_NAME)
    }

    /**
     * [ComparableBinding] for [PQIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream) = PQIndexConfig(
            Name.FunctionName(StringBinding.BINDING.readObject(stream)),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<PQIndexConfig>) {
            require(`object` is PQIndexConfig) { "PQIndexConfig.Binding can only be used to serialize instances of PQIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.numCentroids)
            IntegerBinding.writeCompressed(output, `object`.subspaces)
            IntegerBinding.writeCompressed(output, `object`.seed)
        }
    }



    init {
        /* Range of sanity checks. */
        require(this.numCentroids > 0) { "PQIndex requires at least one centroid." }
        require(this.numCentroids <= Short.MAX_VALUE) { "PQIndex supports a maximum number of ${Short.MAX_VALUE} centroids." }
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
        var subspaces = this.subspaces
        do {
            if (d % subspaces == 0) return subspaces
        } while ((++subspaces) <= MAXIMUM_NUMBER_OF_SUBSPACES)

        /* We have to try lower; which will increase distance distortion. */
        subspaces = this.subspaces
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
        KEY_SEED to this.seed.toString(),
    )
}