package org.vitrivr.cottontail.functions.exception

import org.vitrivr.cottontail.functions.basics.Signature

/**
 * A exception thrown by the [FunctionGenerator] if generating a certain [Function] fails.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FunctionNotSupportedException(signature: Signature.Open<*>): Throwable("Function with signature $signature is not supported by this function generator.")