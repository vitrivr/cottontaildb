package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface Value<T> {
    /** Actual value of this [Value]. */
    val value: T

    /** Size of this [Value]. */
    val size: Int

    /** True if this is a numeric [Value]. */
    val numeric: Boolean

    /**
     * Comparison operator between two values. Returns -1, 0 or 1 of other value is smaller, equal or greater than this value.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    operator fun compareTo(other: Value<*>): Int
}