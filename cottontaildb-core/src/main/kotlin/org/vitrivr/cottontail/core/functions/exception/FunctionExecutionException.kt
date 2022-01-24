package org.vitrivr.cottontail.core.functions.exception

import org.vitrivr.cottontail.core.functions.Signature

/**
 * An exception thrown by a [Func] if a function lookup fails.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FunctionExecutionException(signature: Signature.Closed<*>, message: String): Throwable("Failed to execute function $signature: $message")