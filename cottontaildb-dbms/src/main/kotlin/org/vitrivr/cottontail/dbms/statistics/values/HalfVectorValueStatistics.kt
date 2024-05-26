package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.HalfVectorValue

/**
 * A [ValueStatistics] implementation for [HalfVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class HalfVectorValueStatistics(
    logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: HalfVectorValue = HalfVectorValue(FloatArray(logicalSize) { Float.MAX_VALUE }),
    override val max: HalfVectorValue = HalfVectorValue(FloatArray(logicalSize) { Float.MIN_VALUE }),
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))
) : RealVectorValueStatistics<HalfVectorValue>(Types.HalfVector(logicalSize))