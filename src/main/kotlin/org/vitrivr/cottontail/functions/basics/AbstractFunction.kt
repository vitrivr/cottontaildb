package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.model.values.types.Value

/**
 * An abstract function implementation that implements storage and application of function arguments.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractFunction<R: Value>(final override val signature: Signature.Closed<out R>) : Function<R> {
    /** Array of arguments used by this [AbstractFunction]. */
    protected val arguments: Array<Value?> = arrayOfNulls(this.signature.arguments.size)

    /**
     * Provides a positional argument [Value] for this [Function].
     *
     * @param index The index of the argument.
     * @param value The argument value.
     */
    override fun provide(index: Int, value: Value?) = arguments.forEach { arg ->
        require(this.signature.arguments[index].isCompatible(value)) {
            "Provided argument for position ${index} is not compatible with signature ${this.signature}."
        }
        this.arguments[index] = value
    }
}