package org.vitrivr.cottontail.dbms.statistics.selectivity

/**
 * Represents the [Selectivity] of (usually) a predicate.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class Selectivity(private val value: Float) {
    companion object {
        /** Default [Selectivity]. This is a worst-case assumption in case no better estimate exists. */
        val DEFAULT = Selectivity(0.98f)

        /** [Selectivity] if noting is selected. */
        val NOTHING = Selectivity(0.0f)

        /** [Selectivity] if noting is selected. */
        val ALL = Selectivity(1.0f)
    }
    /**
     * Calculates and returns the output size given this [Selectivity] and the input size.
     *
     * @param input The
     */
    operator fun invoke(input: Long): Long {
        require(input >= 0) { "The input size must be greater than or equal to zero." }
        return (input * this.value).toLong()
    }

    operator fun plus(other: Selectivity) = Selectivity(this.value + other.value)
    operator fun minus(other: Selectivity) = Selectivity(this.value - other.value)
    operator fun times(other: Selectivity) = Selectivity(this.value * other.value)
    operator fun div(other: Selectivity) = Selectivity(this.value / other.value)
}