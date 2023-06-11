package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.NumericValue

/**
 * A [ValueGenerator] for [NumericValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface NumericValueGenerator<T: NumericValue<*>>: ValueGenerator<T> {
    /**
     * Generates a [NumericValue] one.
     *
     * @return [NumericValue] one.
     */
    fun one(): T

    /**
     * Generates a [NumericValue] zero.
     *
     * @return [NumericValue] zero.
     */
    fun zero(): T

    /**
     * Generates a [NumericValue] of the given [Number].
     *
     * @return [NumericValue] zero.
     */
    fun of(number: Number): T
}