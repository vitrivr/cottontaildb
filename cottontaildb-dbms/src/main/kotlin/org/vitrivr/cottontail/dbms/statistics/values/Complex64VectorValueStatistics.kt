package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue

/**
 * A [ValueStatistics] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Complex64VectorValueStatistics(
    val logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L
): AbstractVectorStatistics<Complex64VectorValue>(Types.Complex64Vector(logicalSize))