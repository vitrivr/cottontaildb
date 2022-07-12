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
data class PQIndexConfig(val distance: Name.FunctionName, val numCentroids: Int, val seed: Int = System.currentTimeMillis().toInt()) : IndexConfig<PQIndex> {

    companion object {
        /** Configuration key for the number of subspaces. */
        const val KEY_DISTANCE = "distance"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_CENTROIDS = "num_centroids"

        /** Configuration key for the sample size. */
        const val KEY_SAMPLE_SIZE = "sample_rate"

        /** DEfault value for the number of centroids as recommended by Jegou et al. */
        const val DEFAULT_CENTROIDS = 256

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
            IntegerBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<PQIndexConfig>) {
            require(`object` is PQIndexConfig) { "PQIndexConfig.Binding can only be used to serialize instances of PQIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.numCentroids)
            IntegerBinding.writeCompressed(output, `object`.seed)
        }
    }

    init {
        /* Range of sanity checks. */
        require(this.numCentroids > 0) { "PQIndex requires at least one centroid." }
        require(this.numCentroids <= Short.MAX_VALUE) { "PQIndex supports a maximum number of ${Short.MAX_VALUE} centroids." }
        require(this.distance in SUPPORTED_DISTANCES) { "PQIndex only support L1, L2, L2SQUARED, COSINE and INNERPRODUCT distance."}
    }
}