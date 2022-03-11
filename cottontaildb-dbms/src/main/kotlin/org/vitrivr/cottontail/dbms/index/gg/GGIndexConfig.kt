package org.vitrivr.cottontail.dbms.index.gg

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.functions.math.distance.Distances
import org.vitrivr.cottontail.dbms.index.IndexConfig
import java.io.ByteArrayInputStream

/**
 * Configuration class for [GGIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.3.0
 */
data class GGIndexConfig(val numGroups: Int, val seed: Long, val distance: Distances) : IndexConfig<GGIndex> {

    /**
     *
     */
    companion object {
        const val KEY_NUM_SUBSPACES_KEY = "num_groups"
        const val KEY_SEED_KEY = "seed"
        const val KEY_DISTANCE_KEY = "distance"
    }

    /**
     * [ComparableBinding] for [GGIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<GGIndexConfig> = GGIndexConfig(
            IntegerBinding.readCompressed(stream),
            LongBinding.readCompressed(stream),
            Distances.values()[IntegerBinding.readCompressed(stream)],
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<GGIndexConfig>) {
            require(`object` is GGIndexConfig) { "GGIndexConfig.Binding can only be used to serialize instances of GGIndexConfig." }
            IntegerBinding.writeCompressed(output, `object`.numGroups)
            LongBinding.writeCompressed(output, `object`.seed)
            IntegerBinding.writeCompressed(output, `object`.distance.ordinal)
        }
    }
}