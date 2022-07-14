package org.vitrivr.cottontail.dbms.statistics.columns

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types

/**
 * A [ValueStatistics] implementation for [RealValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class RealValueStatistics<T: RealValue<*>>(type: Types<T>): AbstractValueStatistics<T>(type) {
    companion object {
        const val MIN_KEY = "min"
        const val MAX_KEY = "max"
        const val SUM_KEY = "sum"
        const val MEAN_KEY = "mean"
    }

    /** Minimum value seen by this [RealValueStatistics]. */
    abstract val min: T

    /** Minimum value seen by this [RealValueStatistics]. */
    abstract val max: T

    /** Sum of all values seen by this [RealValueStatistics]. */
    abstract val sum: DoubleValue

    /** The arithmetic mean for the values seen by this [RealValueStatistics]. */
    val mean: DoubleValue
        get() =  DoubleValue(this.sum.value / this.numberOfNonNullEntries)

    /**
     * Creates a descriptive map of this [RealValueStatistics].
     *
     * @return Descriptive map of this [RealValueStatistics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        MIN_KEY to this.min.value.toString(),
        MAX_KEY to this.max.value.toString(),
        SUM_KEY to this.sum.value.toString(),
        MEAN_KEY to this.mean.value.toString()
    )
}