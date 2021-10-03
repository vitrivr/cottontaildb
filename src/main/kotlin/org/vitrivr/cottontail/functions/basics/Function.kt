package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.model.values.types.Value

/**
 * An invokable [Function] that can be used by Cottontail DB to produce results.
 *
 * A function is a piece of logic, that takes an arbitrary number of [Value]s as input returns some [Value]
 * as output. A [Function] is fully defined by its [Signature].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Function<out R: Value> {
    /** Signature of this [Function]. */
    val signature: Signature.Closed<out R>

    /** Cost of calling this [Function]. */
    val cost: Float

    /** Flag indicating, that this [Function] can be executed. Defaults to true. */
    val executable: Boolean
        get() = true

    /**
     * Invokes this [Function] with the given arguments.
     *
     * @param arguments Arguments of type [Value].
     * @return [R]
     */
    operator fun invoke(vararg arguments: Value?): R

    /**
     * [Function.Stateless] are [Function]s that do not have any state and are therefore safe for re-use.
     *
     * Usually, only a single instance of a [Function.Stateless] exists within Cottontail DB.
     */
    interface Stateless<out R: Value>: Function<R>

    /**
     * [Function.Stateful] are [Function]s that have an internal state, e.g., values that remain constant between function invocations.
     *
     */
    interface Stateful<out R: Value>: Function<R> {

        /** [IntArray] containing the positions of the stateful arguments w.r.t to this [Function.Stateful]'s [Signature]. */
        val statefulArguments: IntArray

        /**
         * Prepares this [Function.Stateful] for executing by applying the provided arguments.
         *
         * The order in which arguments must be provided is determined by the [Function.Stateful]'s signature.
         */
        fun prepare(vararg arguments: Value?)
    }
}


