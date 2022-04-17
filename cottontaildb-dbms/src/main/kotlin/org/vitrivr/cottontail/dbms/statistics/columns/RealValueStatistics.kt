package org.vitrivr.cottontail.dbms.statistics.columns

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealValue

/**
 * A [ValueStatistics] implementation for [RealValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface RealValueStatistics<T: RealValue<*>>: ValueStatistics<T> {
    /** Minimum value seen by this [RealValueStatistics]. */
    val min: T

    /** Minimum value seen by this [RealValueStatistics]. */
    val max: T

    /** Sum of all values seen by this [RealValueStatistics]. */
    val sum: DoubleValue

    /** The arithmetic mean for the values seen by this [RealValueStatistics]. */
    val mean: DoubleValue
        get() =  DoubleValue(this.sum.value / this.numberOfNonNullEntries)
}