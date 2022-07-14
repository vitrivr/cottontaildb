package org.vitrivr.cottontail.dbms.index.ivfpq

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
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import java.io.ByteArrayInputStream

/**
 * Configuration claess for [IVFPQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.3.0
 */
class IVFPQIndexConfig(val distance: Name.FunctionName, val numCentroids: Int, val numCoarseCentroids: Int, val seed: Int = System.currentTimeMillis().toInt()) : IndexConfig<IVFPQIndex> {

    companion object {
        /** Configuration key for the number of subspaces. */
        const val KEY_DISTANCE = "ivfpq.distance"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_CENTROIDS = "ivfpq.centroids"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_COARSE_CENTROIDS = "ivfpq.coarse_centroids"

        /** Configuration key for the number of centroids. */
        const val KEY_SEED = "ivfpq.seed"

        /** DEfault value for the number of centroids as recommended by Jegou et al. */
        const val DEFAULT_CENTROIDS = 256

        /** Set of supported distances. */
        val SUPPORTED_DISTANCES: Set<Name.FunctionName> = setOf(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, SquaredEuclideanDistance.FUNCTION_NAME, CosineDistance.FUNCTION_NAME)
    }

    /**
     * [ComparableBinding] for [IVFPQIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream) = IVFPQIndexConfig(
            Name.FunctionName(StringBinding.BINDING.readObject(stream)),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<IVFPQIndexConfig>) {
            require(`object` is IVFPQIndexConfig) { "IVFPQIndexConfig.Binding can only be used to serialize instances of IVFPQIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.numCentroids)
            IntegerBinding.writeCompressed(output, `object`.numCoarseCentroids)
            IntegerBinding.writeCompressed(output, `object`.seed)
        }
    }

    init {
        /* Range of sanity checks. */
        require(this.numCentroids > 0) { "IVFPQIndex requires at least one centroid." }
        require(this.numCoarseCentroids > 0) { "IVFPQIndex requires at least one centroid." }
        require(this.numCentroids <= Short.MAX_VALUE) { "IVFPQIndex supports a maximum number of ${Short.MAX_VALUE} coarse centroids." }
        require(this.numCoarseCentroids <= Byte.MAX_VALUE) { "IVFPQIndex supports a maximum number of ${Short.MAX_VALUE} coarse centroids." }
        require(this.distance in SUPPORTED_DISTANCES) { "PQIndex only support L1, L2, L2SQUARED, COSINE and INNERPRODUCT distance."}
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