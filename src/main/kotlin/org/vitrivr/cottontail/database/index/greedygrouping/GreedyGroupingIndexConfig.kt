package org.vitrivr.cottontail.database.index.greedygrouping

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

data class GreedyGroupingIndexConfig(val numGroups: Int, val seed: Long) {
    companion object Serializer: org.mapdb.Serializer<GreedyGroupingIndexConfig> {
        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         *
         * @throws IOException in case of an I/O error
         */
        override fun serialize(out: DataOutput2, value: GreedyGroupingIndexConfig) {
            out.packInt(value.numGroups)
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
        override fun deserialize(input: DataInput2, available: Int) = GreedyGroupingIndexConfig(
                numGroups = input.unpackInt(),
                seed = input.unpackLong()
        )

        fun fromParamsMap(params: Map<String, String>) =
                GreedyGroupingIndexConfig(
                        numGroups = (params[GreedyGroupingIndexConfigParamMapKeys.NUM_GROUPS.key]
                                ?: error("'${GreedyGroupingIndexConfigParamMapKeys.NUM_GROUPS.key}' not found")).toInt(),
                        seed = (params[GreedyGroupingIndexConfigParamMapKeys.SEED.key]
                                ?: error("'${GreedyGroupingIndexConfigParamMapKeys.SEED.key}' not found")).toLong(),
                )
    }
}