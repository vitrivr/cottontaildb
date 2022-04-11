package org.vitrivr.cottontail.dbms.statistics.columns

import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [ValueStatistics] for [VectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface VectorValueStatistics<T: VectorValue<*>>: ValueStatistics<T> {
    /** The element-wise, minimum value seen by this [VectorValueStatistics]. */
    val min: T

    /** The element-wise, maximum value seen by this [VectorValueStatistics]. */
    val max: T

    /** The element-wise, arithmetic mean for the values seen by this [VectorValueStatistics]. */
    val mean: T

    /** The element-wise, sum of all values in this [VectorValueStatistics]. */
    val sum: T
}