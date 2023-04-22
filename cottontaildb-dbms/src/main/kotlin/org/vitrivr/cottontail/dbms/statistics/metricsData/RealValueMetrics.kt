package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types

/**
 * A [ValueMetrics] implementation for [RealValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class RealValueMetrics<T: RealValue<*>>(type: Types<T>): AbstractScalarMetrics<T>(type) {
    companion object {
        const val MIN_KEY = "min"
        const val MAX_KEY = "max"
        const val SUM_KEY = "sum"
        const val MEAN_KEY = "mean"
    }

    /** Minimum value seen by this [RealValueMetrics]. */
    abstract val min: T

    /** Minimum value seen by this [RealValueMetrics]. */
    abstract val max: T

    /** Sum of all values seen by this [RealValueMetrics]. */
    abstract val sum: DoubleValue

    /** The arithmetic mean for the values seen by this [RealValueMetrics]*/
    abstract val mean : DoubleValue

    /** The variance for the values seen by this [RealValueMetrics]*/
    abstract val variance : DoubleValue

    /** The skewness for the values seen by this [RealValueMetrics]*/
    abstract val skewness : DoubleValue

    /** The kurtosis for the values seen by this [RealValueMetrics]*/
    abstract val kurtosis : DoubleValue

    /**
     * Creates a descriptive map of this [RealValueMetrics].
     *
     * @return Descriptive map of this [RealValueMetrics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        MIN_KEY to this.min.value.toString(),
        MAX_KEY to this.max.value.toString(),
        SUM_KEY to this.sum.value.toString(),
        MEAN_KEY to this.mean.value.toString()
    )
}