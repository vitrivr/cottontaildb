package org.vitrivr.cottontail.database.index.va

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.math.knn.metrics.Distances

/**
 * A configuration class for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class VAFIndexConfig(val marksPerDimension: Int, val kernel: Distances) {
    companion object Serializer : org.mapdb.Serializer<VAFIndexConfig> {
        const val MARKS_PER_DIMENSION_KEY = "marks_per_dimension"
        const val KERNEL_KEY = "kernel"

        override fun serialize(out: DataOutput2, value: VAFIndexConfig) {
            out.packInt(value.marksPerDimension)
            out.packInt(value.kernel.ordinal)
        }

        override fun deserialize(input: DataInput2, available: Int) = VAFIndexConfig(
                input.unpackInt(),
                Distances.values()[input.unpackInt()]
        )

        /**
         * Constructs a [VAFIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return VAFIndexConfig
         */
        fun fromParamMap(params: Map<String, String>) = VAFIndexConfig(params[MARKS_PER_DIMENSION_KEY]!!.toInt(), Distances.valueOf(params[KERNEL_KEY]!!))
    }
}
