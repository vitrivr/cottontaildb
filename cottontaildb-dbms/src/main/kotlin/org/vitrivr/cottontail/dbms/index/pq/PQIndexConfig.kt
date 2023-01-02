package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.dbms.index.IndexConfig
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

/**
 * Configuration class for [PQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.2.0
 */
data class PQIndexConfig(val distance: Name.FunctionName, val sampleSize: Int, val numCentroids: Int, val numSubspaces: Int? = null, val seed: Long = System.currentTimeMillis(), val centroids: List<DoubleArray> = emptyList()) : IndexConfig<PQIndex> {

    companion object {
        /** Configuration key for the number of subspaces. */
        const val KEY_DISTANCE = "distance"

        /** Configuration key for the number of subspaces. */
        const val KEY_NUM_SUBSPACES = "num_subspaces"

        /** Configuration key for the number of centroids. */
        const val KEY_NUM_CENTROIDS = "num_centroids"

        /** Configuration key for the sample size. */
        const val KEY_SAMPLE_SIZE = "sample_size"

        /** Set of supported distances. */
        val SUPPORTED_DISTANCES: Set<Name.FunctionName> = setOf(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, SquaredEuclideanDistance.FUNCTION_NAME, CosineDistance.FUNCTION_NAME)
    }

    /**
     * [ComparableBinding] for [PQIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<PQIndexConfig> {
            val distance = Name.FunctionName.create(StringBinding.BINDING.readObject(stream))
            val sampleSize = IntegerBinding.readCompressed(stream)
            val numCentroids = IntegerBinding.readCompressed(stream)
            val numSubspaces = IntegerBinding.readCompressed(stream)
            val seed = LongBinding.readCompressed(stream)
            val actualNumberOfCentroids = IntegerBinding.readCompressed(stream)
            val centroids = (0 until actualNumberOfCentroids).map {
                Snappy.uncompressDoubleArray(stream.readNBytes(IntegerBinding.readCompressed(stream)))
            }
            return PQIndexConfig(distance, sampleSize, numCentroids, if (numSubspaces == -1) { null } else { numSubspaces }, seed, centroids)
        }

        override fun writeObject(output: LightOutputStream, `object`: Comparable<PQIndexConfig>) {
            require(`object` is PQIndexConfig) { "PQIndexConfig.Binding can only be used to serialize instances of PQIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.sampleSize)
            IntegerBinding.writeCompressed(output, `object`.numCentroids)
            IntegerBinding.writeCompressed(output, `object`.numSubspaces ?: -1)
            LongBinding.writeCompressed(output, `object`.seed)
            IntegerBinding.writeCompressed(output, `object`.centroids.size)
            for (c in `object`.centroids) {
                val compressed = Snappy.compress(c)
                IntegerBinding.writeCompressed(output, compressed.size)
                output.write(compressed)
            }
        }
    }

    init {
        /* Range of sanity checks. */
        require(this.numCentroids > 0) { "PQIndex requires at least one centroid." }
        require(this.numSubspaces == null || this.numSubspaces > 0) { "PQIndex requires at least one sub space." }
        require(this.numCentroids <= Short.MAX_VALUE) { "PQIndex supports a maximum number of ${Short.MAX_VALUE} centroids." }
        require(this.distance in SUPPORTED_DISTANCES) { "PQIndex only support L1, L2, L2SQUARED, COSINE and INNERPRODUCT distance."}
    }
}