package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue

/**
 * A [ValueStatistics] for [RealVectorValue]s
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

    /** The component-wise, minimum value seen by this [RealVectorValueStatistics]. */
    abstract val min: T

    /** The component-wise, maximum value seen by this [RealVectorValueStatistics]. */
    abstract val max: T

    /** The component-wise, sum of all values in this [RealVectorValueStatistics]. */
    abstract val sum: DoubleVectorValue

    /** The arithmetic for the values seen by this [RealVectorValueStatistics]. */
    val mean: DoubleVectorValue by lazy {
        DoubleVectorValue(DoubleArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })
    }

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