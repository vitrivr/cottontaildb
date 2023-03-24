package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [ValueMetrics] for [VectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class RealVectorValueMetrics<T: RealVectorValue<*>>(type: Types<T>): AbstractValueMetrics<T>(type) {
    companion object {
        const val MIN_KEY = "cmin"
        const val MAX_KEY = "cmax"
        const val SUM_KEY = "csum"
        const val MEAN_KEY = "cmean"
    }

    /** The element-wise, minimum value seen by this [RealVectorValueMetrics]. */
    abstract val min: T

    /** The element-wise, maximum value seen by this [RealVectorValueMetrics]. */
    abstract val max: T

    /** The element-wise, arithmetic mean for the values seen by this [RealVectorValueMetrics]. */
    abstract val mean: T

    /** The element-wise, sum of all values in this [RealVectorValueMetrics]. */
    abstract val sum: T

    /**
     * Creates a descriptive map of this [RealVectorValueMetrics].
     *
     * @return Descriptive map of this [RealVectorValueMetrics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        MIN_KEY to this.min.joinToString(",", "[", "]") { it.value.toString() },
        MAX_KEY to this.max.joinToString(",", "[", "]") { it.value.toString() },
        SUM_KEY to this.sum.joinToString(",", "[", "]") { it.value.toString() },
        MEAN_KEY to this.mean.joinToString(",", "[", "]") { it.value.toString() }
    )
}