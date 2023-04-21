package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.values.types.Value

/**
 * This decorator is part of Cottontail DB's vectorisation feature.
 *
 * [Function]s implementing this interface signal to the DBMS, that they can be vectorised.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface VectorisableFunction<out R: Value>: Function<R> {
    /**
     * This is an estimate of the vector size this [VectorisableFunction] processing.
     *
     * It is used as a hint to decide whether vectorisation makes sense.
     */
    val vectorSize: Int

    /**
     * Returns the [VectorisedFunction] version of this [VectorisableFunction].
     *
     * @return [VectorisedFunction]
     */
    fun vectorized(): VectorisedFunction<R>
}