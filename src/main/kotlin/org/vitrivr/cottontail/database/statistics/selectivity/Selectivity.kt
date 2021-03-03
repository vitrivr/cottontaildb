package org.vitrivr.cottontail.database.statistics.selectivity

/**
 * Represents the [Selectivity] of (usually) a predicate.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */

inline class Selectivity(private val value: Float) {
    companion object {
        val DEFAULT_SELECTIVITY = Selectivity(0.98f)
    }

    init {
        /* Sanity checks; [Selectivity]s are like probabilities. */
        require(this.value in 0.0f..1.0f) { "Selectivity value must be in the range between 0.0 and 1.0. " }
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