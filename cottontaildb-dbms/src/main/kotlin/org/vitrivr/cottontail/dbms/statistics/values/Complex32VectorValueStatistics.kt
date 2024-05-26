package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue

/**
 * A [ValueStatistics] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class Complex32VectorValueStatistics(
    logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
): AbstractVectorStatistics<Complex32VectorValue>(Types.Complex32Vector(logicalSize))