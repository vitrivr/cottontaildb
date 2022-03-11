package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.index.IndexConfig
import java.io.ByteArrayInputStream

/**
 * Configuration class for [PQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.2.0
 */
data class PQIndexConfig(val numSubspaces: Int, val numCentroids: Int, val sampleSize: Int, val seed: Long, val pq: PQ? = null) : IndexConfig<PQIndex> {

    companion object {
        const val AUTO_VALUE = -1
        const val NUM_SUBSPACES_KEY = "num_subspaces"
        const val NUM_CENTROIDS_KEY = "num_centroids"
        const val SAMPLE_SIZE = "sample_size"
        const val SEED_KEY = "seed"
    }

    /**
     * [ComparableBinding] for [PQIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<PQIndexConfig> = PQIndexConfig(
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            LongBinding.readCompressed(stream),
            if (BooleanBinding.BINDING.readObject(stream)) {
                PQ.Binding.readObject(stream)
            } else {
                null
            }
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<PQIndexConfig>) {
            require(`object` is PQIndexConfig) { "PQIndexConfig.Binding can only be used to serialize instances of PQIndexConfig." }
            IntegerBinding.writeCompressed(output, `object`.numSubspaces)
            IntegerBinding.writeCompressed(output, `object`.numCentroids)
            IntegerBinding.writeCompressed(output, `object`.sampleSize)
            LongBinding.writeCompressed(output, `object`.seed)
            if (`object`.pq != null) {
                BooleanBinding.BINDING.writeObject(output, true)
                PQ.Binding.writeObject(output, `object`.pq)
            } else {
                BooleanBinding.BINDING.writeObject(output, false)
            }
        }
    }
}