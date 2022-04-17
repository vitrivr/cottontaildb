package org.vitrivr.cottontail.core.queries.nodes

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext

/**
 * A [Node] that can be bound to a [BindingContext].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface BindableNode: Node {

    /**
     * Binds all [Binding]s contained in this [Node] to the new [BindingContext].
     *
     * @param context The new [BindingContext] to bind [Binding]s to.
     */
    fun bind(context: BindingContext)
}