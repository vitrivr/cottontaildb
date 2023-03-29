package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.values.types.Value

/**
 * This decorator is part of Cottontail DB's vectorisation feature.
 *
 * [Function]s implementing this interface signal to the DBMS, that they have been vectorised.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface VectorisedFunction<out R: Value>: Function<R>