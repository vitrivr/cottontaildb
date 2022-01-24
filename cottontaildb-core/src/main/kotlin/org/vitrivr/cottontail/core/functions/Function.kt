package org.vitrivr.cottontail.core.functions

import org.vitrivr.cottontail.core.values.types.Value
import kotlin.Function

/**
 * An invokable [Function] that can be used by Cottontail DB to produce results. A [Function] is fully defined by its [Signature].
 *
 * A function is a piece of logic, that takes an arbitrary number of [Value]s as input returns some [Value]  as output. In practice,
 * this is executed in two steps: First, arguments can be provided by [Function.provide] then the [Function] can be invoked by [Function.invoke].
 * The reason for separating these steps is, that in a query plan, some arguments may remain static while others are dynamically determined as
 * the query is executed.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Function<out R: Value> {
    /** Signature of this [Function]. */
    val signature: Signature.Closed<out R>

    /** Cost of calling this [Function]. */
    val cost: Float

    /** Flag indicating, that this [Function] can be executed. Defaults to true. */
    val executable: Boolean
        get() = true

    /**
     * Invokes this [Function] and returns the results.
     *
     * @return [R]
     */
    operator fun invoke(): R

    /**
     * Provides a positional argument [Value] for this [Function].
     *
     * @param index The index of the argument.
     * @param value The argument value.
     */
    fun provide(index: Int, value: Value?)
}


