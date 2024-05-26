package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.ShortVectorValue

/**
 * A [ValueStatistics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class ShortVectorValueStatistics(
    logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override val min: ShortVectorValue = ShortVectorValue(ShortArray(logicalSize) { Short.MAX_VALUE }),
    override val max: ShortVectorValue = ShortVectorValue(ShortArray(logicalSize) { Short.MIN_VALUE }),
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))
): RealVectorValueStatistics<ShortVectorValue>(Types.ShortVector(logicalSize))