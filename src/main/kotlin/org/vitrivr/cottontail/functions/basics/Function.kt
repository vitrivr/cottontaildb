package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.model.values.types.Value

/**
 * A invokable [Function] that can be used by Cottontail DB to produce results.
 *
 * A function is a bit of logic, that takes an arbitrary number of [Value]s as input returns some [Value]
 * a output. A [Function] is fully defined by its [Signature].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Function<out R: Value> {
    /** Signature of this [Function]. */
    val signature: Signature.Closed<out R>

    /** Cost of calling this [Function]. */
    val cost: Float

    /**
     * Invokes this [Function] with the given arguments.
     *
     * @param arguments Arguments of type [Value].
     * @return [R]
     */
    fun invoke(vararg arguments: Value): R

    /**
     * [Function.Static] are [Function]s that are known ahead of time upon booting up Cottontail DB.
     *
     * They cannot have any state and are therefore safe for re-use.
     */
    interface Static<out R: Value>: Function<R> {

    }

    /**
     * [Function.Dynamic] are [Function]s that are not known ahead of time and generated when executing a certain query.
     *
     * The rely on [FunctionGenerator]s to do so. Furthermore, [Function.Dynamic] may have a state and can therefore not be re-used.
     */
    interface Dynamic<out R: Value>: Function<R> {
        /** Flag indicating whether this [Function] is stateful (i.e., has local variables) or stateless. */
        val stateless: Boolean
    }
}


