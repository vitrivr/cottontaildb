package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Abstract [FunctionGenerator] implementation
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractFunctionGenerator<out R: Value>: FunctionGenerator<R> {
    /**
     * Generates a [Function.Dynamic] for the given arguments.
     *
     * @param arguments The argument [Value]s to generate a [Function] for.
     * @return The generated [Function.Dynamic]
     */
    final override fun generate(vararg arguments: Type<*>): Function.Dynamic<R>  {
        if (arguments.size != this.signature.arity) throw FunctionNotSupportedException(Signature.Open(this.signature.name, this.signature.returnType, arguments.size))
        val ret = this.generateInternal(*arguments)
        require (ret.signature.arguments.size == this.signature.arity) { "Number of arguments of the produced function does not match arity of function generator. This is a programmer's error!" }
        return ret
    }

    /**
     * Internal [Function.Dynamic] generator.
     *
     * @param arguments The argument [Value]s to generate a [Function] for.
     * @return The generated [Function.Dynamic]
     */
    protected abstract fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<R>
}