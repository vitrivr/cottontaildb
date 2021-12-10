package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Abstract [FunctionGenerator] implementation
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
abstract class AbstractFunctionGenerator<out R: Value>: FunctionGenerator<R> {
    /**
     * Generates a [Function.Stateful] for the given arguments.
     *
     * @param arguments The argument [Value]s to generate a [Function] for.
     * @return The generated [Function.Stateful]
     */
    final override fun generate(vararg arguments: Argument.Typed<*>): Function<R>  {
        /* Check compatibility. */
        val destSignature = Signature.Closed(this.signature.name, arguments.map { Argument.Typed(it.type) }.toTypedArray(), this.signature.returnType)
        if (!this.signature.includes(destSignature)) {
            throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $destSignature).")
        }

        /* Check of closes signature of this Function is compatible with arguments. */
        val ret = this.generateInternal(destSignature)
        if (ret.signature != destSignature) {
            throw FunctionNotSupportedException("Generated signature ${ret.signature} is not compatible with destination signature (dst = $destSignature)")
        }

        return ret
    }

    /**
     * Internal [Function.Stateful] generator.
     *
     * @param dst The destination signature [Signature.Closed].
     * @return The generated [Function.Stateful]
     */
    protected abstract fun generateInternal(dst: Signature.Closed<*>): Function<R>
}