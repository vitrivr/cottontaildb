package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue

/**
 * A [ValueStatistics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class IntValueStatistics (
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: IntValue = IntValue.MAX_VALUE,
    override val max: IntValue = IntValue.MIN_VALUE,
    override val sum: DoubleValue = DoubleValue.ZERO,
    override val mean: DoubleValue = DoubleValue.ZERO,
    override val variance: DoubleValue = DoubleValue.ZERO,
    override val skewness: DoubleValue = DoubleValue.ZERO,
    override val kurtosis: DoubleValue = DoubleValue.ZERO
) : RealValueStatistics<IntValue>(Types.Int)