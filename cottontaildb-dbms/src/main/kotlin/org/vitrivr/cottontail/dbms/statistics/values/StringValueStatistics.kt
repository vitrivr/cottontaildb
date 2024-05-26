package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue

/**
 * A specialized [ValueStatistics] implementation for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class StringValueStatistics(
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override var minWidth: Int = Int.MAX_VALUE,
    override var maxWidth: Int = Int.MIN_VALUE
) : AbstractScalarStatistics<StringValue>(Types.String) {



    /**
     * Creates a descriptive map of this [ValueStatistics].
     *
     * @return Descriptive map of this [ValueStatistics]
     */
    override fun about(): Map<String, String> {
        return super.about() + mapOf(
            MIN_WIDTH_KEY to this.minWidth.toString(),
            MAX_WIDTH_KEY to this.maxWidth.toString()
        )
    }
}