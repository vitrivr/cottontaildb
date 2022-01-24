package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.values.types.NumericValue
import org.vitrivr.cottontail.core.values.types.ScalarValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import java.util.*

/**
 * This is an abstraction over a [Date].
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
@JvmInline
value class DateValue(override val value: Long) : ScalarValue<Long> {

    /**
     * Converts a [Date] to a [DateValue].
     *
     * @param date The [Date] to convert.
     */
    constructor(date: Date) : this(date.time)

    /** The logical size of this [DateValue]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [DateValue]. */
    override val type: Types<*>
        get() = Types.Date

    /**
     * Compares this [LongValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [LongValue] can only be compared to other [NumericValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is DateValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("DateValue can only be compared to other DateValues.")
    }

    override fun isEqual(other: Value): Boolean =
        (other is DateValue) && (other.value == this.value)
}