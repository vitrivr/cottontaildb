package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
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
        const val VARIANCE_KEY = "variance"
        const val SKEWNESS_KEY = "skewness"
        const val KURTOSIS_KEY = "kurtosis"
    }

    /** Minimum value seen by this [RealValueMetrics]. */
    abstract var min: T

    /** Minimum value seen by this [RealValueMetrics]. */
    abstract var max: T

    /** Sum of all values seen by this [RealValueMetrics]. */
    abstract var sum: DoubleValue

    /** The arithmetic mean for the values seen by this [RealValueMetrics]*/
    abstract var mean : DoubleValue

    /** The variance for the values seen by this [RealValueMetrics]*/
    abstract var variance : DoubleValue

    /** The skewness for the values seen by this [RealValueMetrics]*/
    abstract var skewness : DoubleValue

    /** The kurtosis for the values seen by this [RealValueMetrics]*/
    abstract var kurtosis : DoubleValue

    /**
     * Creates a descriptive map of this [RealValueMetrics].
     *
     * @return Descriptive map of this [RealValueMetrics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        MIN_KEY to this.min.value.toString(),
        MAX_KEY to this.max.value.toString(),
        SUM_KEY to this.sum.value.toString(),
        MEAN_KEY to this.mean.value.toString(),
        VARIANCE_KEY to this.variance.value.toString(),
        SKEWNESS_KEY to this.skewness.value.toString(),
        KURTOSIS_KEY to this.kurtosis.value.toString()
    )

    /**
     * Resets this [IntValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        this.sum = DoubleValue.ZERO
        this.mean = DoubleValue.ZERO
        this.variance = DoubleValue.ZERO
        this.skewness = DoubleValue.ZERO
        this.kurtosis = DoubleValue.ZERO
    }
}