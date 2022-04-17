package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.values.types.ScalarValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * This is a [Value] abstraction over a [String].
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
@JvmInline
value class StringValue(override val value: String) : ScalarValue<String> {


    companion object {
        /** The empty [StringValue]. */
        val EMPTY = StringValue("")
    }
    /** The logical size of this [StringValue]. */
    override val logicalSize: Int
        get() = this.value.length

    /** The [Types] of this [StringValue]. */
    override val type: Types<*>
        get() = Types.String

    /**
     * Compares this [StringValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [StringValue] can only be compared to other [StringValue]s.
     *
     * @param other [Value] to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }

    /**
     * Checks for equality between this [StringValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [StringValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is StringValue) && (other.value == this.value)
}