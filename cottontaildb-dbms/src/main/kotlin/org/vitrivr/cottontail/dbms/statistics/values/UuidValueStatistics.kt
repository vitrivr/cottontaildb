package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue

/**
 * A specialized [ValueStatistics] implementation for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class UuidValueStatistics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L
) : AbstractScalarStatistics<UuidValue>(Types.Uuid)