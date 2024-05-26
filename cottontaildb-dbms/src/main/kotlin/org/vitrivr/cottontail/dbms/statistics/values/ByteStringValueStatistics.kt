package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue

/**
 * A specialized [ValueStatistics] implementation for [ByteStringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class ByteStringValueStatistics(
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    override val minWidth: Int = Int.MAX_VALUE,
    override val maxWidth: Int = Int.MIN_VALUE
) : AbstractScalarStatistics<ByteStringValue>(Types.ByteString) {
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