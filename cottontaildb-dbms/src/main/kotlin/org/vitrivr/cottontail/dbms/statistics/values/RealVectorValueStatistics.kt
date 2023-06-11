package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Types

/**
 * A [ValueStatistics] for [VectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class RealVectorValueStatistics<T: RealVectorValue<*>>(type: Types<T>): AbstractVectorStatistics<T>(type) {
    companion object {
        const val MIN_KEY = "cmin"
        const val MAX_KEY = "cmax"
        const val SUM_KEY = "csum"
        const val MEAN_KEY = "cmean"
    }

    /** The element-wise, minimum value seen by this [RealVectorValueStatistics]. */
    abstract val min: T

    /** The element-wise, maximum value seen by this [RealVectorValueStatistics]. */
    abstract val max: T

    /** The element-wise, arithmetic mean for the values seen by this [RealVectorValueStatistics]. */
    abstract val mean: T

    /** The element-wise, sum of all values in this [RealVectorValueStatistics]. */
    abstract val sum: T

    /**
     * Creates a descriptive map of this [RealVectorValueStatistics].
     *
     * @return Descriptive map of this [RealVectorValueStatistics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        MIN_KEY to this.min.joinToString(",", "[", "]") { it.value.toString() },
        MAX_KEY to this.max.joinToString(",", "[", "]") { it.value.toString() },
        SUM_KEY to this.sum.joinToString(",", "[", "]") { it.value.toString() },
        MEAN_KEY to this.mean.joinToString(",", "[", "]") { it.value.toString() }
    )
}