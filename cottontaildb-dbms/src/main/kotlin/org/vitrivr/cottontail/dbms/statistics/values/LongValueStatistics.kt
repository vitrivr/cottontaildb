package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue

/**
 * A [ValueStatistics] implementation for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class LongValueStatistics(
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: LongValue = LongValue.MAX_VALUE,
    override val max: LongValue = LongValue.MIN_VALUE,
    override val sum: DoubleValue = DoubleValue.ZERO,
    override val mean: DoubleValue = DoubleValue.ZERO,
    override val variance: DoubleValue = DoubleValue.ZERO,
    override val skewness: DoubleValue = DoubleValue.ZERO,
    override val kurtosis: DoubleValue = DoubleValue.ZERO
) : RealValueStatistics<LongValue>(Types.Long)