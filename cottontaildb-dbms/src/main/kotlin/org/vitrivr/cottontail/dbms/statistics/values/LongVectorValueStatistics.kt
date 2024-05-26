package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue

/**
 * A [ValueStatistics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class LongVectorValueStatistics(
    logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MAX_VALUE }),
    override val max: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MIN_VALUE }),
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))
): RealVectorValueStatistics<LongVectorValue>(Types.LongVector(logicalSize))