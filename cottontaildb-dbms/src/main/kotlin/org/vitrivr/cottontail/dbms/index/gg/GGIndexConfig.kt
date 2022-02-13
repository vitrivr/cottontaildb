package org.vitrivr.cottontail.dbms.index.gg

import org.vitrivr.cottontail.dbms.functions.math.distance.Distances
import org.vitrivr.cottontail.dbms.index.IndexConfig

/**
 * Configuration class for [GGIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.2.0
 */
data class GGIndexConfig(val numGroups: Int, val seed: Long, val distance: Distances) : IndexConfig {
    companion object {
        const val NUM_SUBSPACES_KEY = "num_groups"
        const val SEED_KEY = "seed"
        const val DISTANCE_KEY = "distance"

        /**
         * Constructs a [GGIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return [GGIndexConfig]
         */
        fun fromParamsMap(params: Map<String, String>) = GGIndexConfig(
            numGroups = params[NUM_SUBSPACES_KEY]?.toIntOrNull() ?: 100,
            seed = params[SEED_KEY]?.toLongOrNull() ?: System.currentTimeMillis(),
            distance = try {
                Distances.valueOf(params[DISTANCE_KEY] ?: "")
            } catch (e: IllegalArgumentException) {
                Distances.L2
            }
        )
    }

    /**
     * Converts this [GGIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [GGIndexConfig].
     */
    override fun toMap(): Map<String, String> = mapOf(
        NUM_SUBSPACES_KEY to this.numGroups.toString(),
        SEED_KEY to this.seed.toString(),
        DISTANCE_KEY to this.distance.toString()
    )
}