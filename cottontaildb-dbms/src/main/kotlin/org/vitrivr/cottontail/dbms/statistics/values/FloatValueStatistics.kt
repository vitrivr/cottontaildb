package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue

/**
 * A [ValueStatistics] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class FloatValueStatistics (
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: FloatValue = FloatValue.MAX_VALUE,
    override val max: FloatValue = FloatValue.MIN_VALUE,
    override val sum: DoubleValue = DoubleValue.ZERO,
    override val mean: DoubleValue = DoubleValue.ZERO,
    override val variance: DoubleValue = DoubleValue.ZERO,
    override val skewness: DoubleValue = DoubleValue.ZERO,
    override val kurtosis: DoubleValue = DoubleValue.ZERO
) : RealValueStatistics<FloatValue>(Types.Float)