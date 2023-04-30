package org.vitrivr.cottontail.core.queries.nodes

import org.vitrivr.cottontail.core.queries.binding.BindingContext

/**
 * A [Node] that must be prepared for query execution.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface PreparableNode: Node  {
    /**
     * Method that is being called directly before query execution starts.
     *
     * Can be used to perform internal setup.
     */
    context(BindingContext)
    fun prepare()
}
