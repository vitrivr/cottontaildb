package org.vitrivr.cottontail.database.index.lsh.superbit

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * Configuration object for [SuperBitLSHIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.0.0
 */
data class SuperBitLSHIndexConfig(val buckets: Int, val stages: Int, val seed: Long, val considerImaginary: Boolean, val samplingMethod: SamplingMethod) {
    companion object Serializer: org.mapdb.Serializer<SuperBitLSHIndexConfig> {


        /**
         *
         */
        override fun serialize(out: DataOutput2, value: SuperBitLSHIndexConfig) {
            out.packInt(value.buckets)
            out.packInt(value.stages)
            out.packLong(value.seed)
            out.packInt(if (value.considerImaginary) 1 else 0)
            out.packInt(value.samplingMethod.ordinal)
        }

        /**
         *
         */
        override fun deserialize(input: DataInput2, available: Int) = SuperBitLSHIndexConfig(
            input.unpackInt(),
            input.unpackInt(),
            input.unpackLong(),
            input.unpackInt() != 0,
            SamplingMethod.values()[input.unpackInt()]
        )

        /**
         * Constructs a [SuperBitLSHIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return SuperBitLSHIndexConfig
         */
        fun fromParamMap(params: Map<String, String>) = SuperBitLSHIndexConfig(
            buckets = params[SuperBitSLHIndexConfigParamMapKeys.NUM_BUCKETS.key]!!.toInt(),
            stages = params[SuperBitSLHIndexConfigParamMapKeys.NUM_STAGES.key]!!.toInt(),
            seed = params[SuperBitSLHIndexConfigParamMapKeys.SEED.key]!!.toLong(),
            considerImaginary = params[SuperBitSLHIndexConfigParamMapKeys.CONSIDER_IMAGINARY.key]!!.toInt() != 0,
            samplingMethod = SamplingMethod.valueOf(params[SuperBitSLHIndexConfigParamMapKeys.SAMPLING_METHOD.key]!!))
    }
}

