package org.vitrivr.cottontail.database.index.gg

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.math.knn.metrics.Distances

/**
 * Configuration class for [GGIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.0.0
 */
data class GGIndexConfig(val numGroups: Int, val seed: Long, val distance: Distances) {
    companion object Serializer : org.mapdb.Serializer<GGIndexConfig> {
        const val NUM_SUBSPACES_KEY = "num_groups"
        const val SEED_KEY = "seed"

        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         */
        override fun serialize(out: DataOutput2, value: GGIndexConfig) {
            out.packInt(value.numGroups)
            out.packLong(value.seed)
            out.packInt(value.distance.ordinal)
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         */
        override fun deserialize(input: DataInput2, available: Int) = GGIndexConfig(
            numGroups = input.unpackInt(),
            seed = input.unpackLong(),
            distance = Distances.values()[input.unpackInt()]
        )

        /**
         * Constructs a [GGIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return [GGIndexConfig]
         */
        fun fromParamsMap(params: Map<String, String>) = GGIndexConfig(
            numGroups = (params[NUM_SUBSPACES_KEY]
                ?: error("'$NUM_SUBSPACES_KEY' not found")).toInt(),
            seed = (params[SEED_KEY] ?: error("'SEED_KEY' not found")).toLong(),
            distance = Distances.L2
        )
    }
}