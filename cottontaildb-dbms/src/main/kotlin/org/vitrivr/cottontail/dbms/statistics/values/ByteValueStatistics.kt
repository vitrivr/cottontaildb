package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [ValueStatistics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class ByteValueStatistics(
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: ByteValue = ByteValue.MAX_VALUE,
    override val max: ByteValue = ByteValue.MIN_VALUE,
    override val sum: DoubleValue = DoubleValue.ZERO,
    override val mean: DoubleValue = DoubleValue.ZERO,
    override val variance: DoubleValue = DoubleValue.ZERO,
    override val skewness: DoubleValue = DoubleValue.ZERO,
    override val kurtosis: DoubleValue = DoubleValue.ZERO
) : RealValueStatistics<ByteValue>(Types.Byte)