package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue

/**
 * A [ValueStatistics] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class IntVectorValueStatistics(
    logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MAX_VALUE }),
    override val max: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MIN_VALUE }),
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))
): RealVectorValueStatistics<IntVectorValue>(Types.IntVector(logicalSize))