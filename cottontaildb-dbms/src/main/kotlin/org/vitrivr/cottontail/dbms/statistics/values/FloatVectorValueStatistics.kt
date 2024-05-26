package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue

/**
 * A [ValueStatistics] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class FloatVectorValueStatistics(
    logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val min: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MAX_VALUE }),
    override val max: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MIN_VALUE }),
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))
) : RealVectorValueStatistics<FloatVectorValue>(Types.FloatVector(logicalSize))