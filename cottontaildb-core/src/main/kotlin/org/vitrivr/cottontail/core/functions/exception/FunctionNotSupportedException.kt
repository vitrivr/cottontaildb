package org.vitrivr.cottontail.core.functions.exception

/**
 * An exception thrown if generating a certain [Function] fails.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FunctionNotSupportedException(message: String): Throwable(message)