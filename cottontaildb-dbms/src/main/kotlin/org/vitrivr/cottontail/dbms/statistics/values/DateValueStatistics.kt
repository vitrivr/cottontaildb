package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue

/**
 * A [ValueStatistics] implementation for [DateValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class DateValueStatistics (
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    val min: DateValue = DateValue(Long.MAX_VALUE),
    val max: DateValue = DateValue(Long.MIN_VALUE),
) : AbstractScalarStatistics<DateValue>(Types.Date) {

    companion object {
        const val MIN_KEY = "min"
        const val MAX_KEY = "max"
    }

    /**
     * Creates a descriptive map of this [ValueStatistics].
     *
     * @return Descriptive map of this [ValueStatistics]
     */
    override fun about(): Map<String, String> {
        return super.about() + mapOf(
            MIN_KEY to this.min.value.toString(),
            MAX_KEY to this.max.value.toString()
        )
    }
}