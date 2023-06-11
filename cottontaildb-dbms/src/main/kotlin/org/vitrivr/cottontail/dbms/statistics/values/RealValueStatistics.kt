package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [ValueStatistics] implementation for [RealValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class RealValueStatistics<T: RealValue<*>>(type: Types<T>): AbstractScalarStatistics<T>(type) {
    companion object {
        const val MIN_KEY = "min"
        const val MAX_KEY = "max"
        const val SUM_KEY = "sum"
        const val MEAN_KEY = "mean"
        const val VARIANCE_KEY = "variance"
        const val SKEWNESS_KEY = "skewness"
        const val KURTOSIS_KEY = "kurtosis"
    }

    /** Minimum value seen by this [RealValueStatistics]. */
    abstract var min: T

    /** Minimum value seen by this [RealValueStatistics]. */
    abstract var max: T

    /** Sum of all values seen by this [RealValueStatistics]. */
    abstract var sum: DoubleValue

    /** The arithmetic mean for the values seen by this [RealValueStatistics]*/
    abstract var mean : DoubleValue

    /** The variance for the values seen by this [RealValueStatistics]*/
    abstract var variance : DoubleValue

    /** The skewness for the values seen by this [RealValueStatistics]*/
    abstract var skewness : DoubleValue

    /** The kurtosis for the values seen by this [RealValueStatistics]*/
    abstract var kurtosis : DoubleValue

    /**
     * Creates a descriptive map of this [RealValueStatistics].
     *
     * @return Descriptive map of this [RealValueStatistics]
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

}