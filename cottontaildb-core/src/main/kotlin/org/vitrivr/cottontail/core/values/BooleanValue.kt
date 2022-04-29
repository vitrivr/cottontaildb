package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.values.types.NumericValue
import org.vitrivr.cottontail.core.values.types.ScalarValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * This is an abstraction over a [Boolean].
 *
 * @author Ralph Gasser
 * @version 1.7.0
 */
@JvmInline
value class BooleanValue(override val value: Boolean): ScalarValue<Boolean> {

    companion object {
        /** The true [BooleanValue]. */
        val TRUE = BooleanValue(true)

        /** The false [BooleanValue]. */
        val FALSE = BooleanValue(false)
    }
    /** The logical size of this [BooleanValue]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] size of this [BooleanValue]. */
    override val type: Types<*>
        get() = Types.Boolean

    /**
     * Compares this [BooleanValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [BooleanValue] can only be compared to other [NumericValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is BooleanValue -> when {
            this.value == other.value -> 0
            this.value -> 1
            else -> -1
        }
        else -> throw IllegalArgumentException("BooleanValue can only be compared to other BooleanValue values.")
    }

    /**
     * Checks for equality between this [BooleanValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [BooleanValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is BooleanValue) && (other.value == this.value)
}