package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value

/**
 * A [ValueStatistics] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Complex32ValueStatistics(
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
): AbstractScalarStatistics<Complex32Value>(Types.Complex32)