package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue

/**
 * A [ValueStatistics] implementation for [BooleanValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class BooleanValueStatistics(
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    val numberOfTrueEntries: Long = 0L,
    val numberOfFalseEntries: Long = 0L
): AbstractScalarStatistics<BooleanValue>(Types.Boolean) {
    companion object {
        const val TRUE_ENTRIES_KEY = "true"
        const val FALSE_ENTRIES_KEY = "false"
    }
    /**
     * Creates a descriptive map of this [BooleanValueStatistics].
     *
     * @return Descriptive map of this [BooleanValueStatistics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        TRUE_ENTRIES_KEY to this.numberOfTrueEntries.toString(),
        FALSE_ENTRIES_KEY to this.numberOfFalseEntries.toString(),
    )
}