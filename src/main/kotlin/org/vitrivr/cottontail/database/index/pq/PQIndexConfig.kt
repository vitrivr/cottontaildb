package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

data class PQIndexConfig(val numSubspaces: Int, val numCentroids: Int, val learningDataFraction: Double, val seed: Long) {

    companion object Serializer : org.mapdb.Serializer<PQIndexConfig> {


        const val NUM_SUBSPACES_KEY = "num_subspaces"
        const val NUM_CENTROIDS_KEY = "num_centroids"
        const val LEARNING_DATA_FRACTION_KEY = "learning_data_fraction"
        const val SEED_KEY = "learning_data_fraction"

        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         *
         * @throws IOException in case of an I/O error
         */
        override fun serialize(out: DataOutput2, value: PQIndexConfig) {
            out.packInt(value.numSubspaces)
            out.packInt(value.numCentroids)
            out.writeDouble(value.learningDataFraction)
            out.packLong(value.seed)
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         * @throws IOException in case of an I/O error
         */
        override fun deserialize(input: DataInput2, available: Int) = PQIndexConfig(
                input.unpackInt(),
                input.unpackInt(),
                input.readDouble(),
                input.unpackLong()
        )

        /**
         * Constructs a [PQIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return [PQIndexConfig]
         */
        fun fromParamMap(params: Map<String, String>) = PQIndexConfig(
                params[NUM_SUBSPACES_KEY]?.toInt() ?: throw IllegalArgumentException(""),
                params[NUM_CENTROIDS_KEY]?.toInt() ?: throw IllegalArgumentException(""),
                params[LEARNING_DATA_FRACTION_KEY]?.toDouble() ?: 0.1,
                params[SEED_KEY]?.toLongOrNull() ?: System.currentTimeMillis()
        )
    }
}