package org.vitrivr.cottontail.core.queries.nodes.traits

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.sort.SortOrder

/**
 * A trait indicating, that the result produced by a query plan sorted.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class OrderTrait(val order: List<Pair<Binding.Column, SortOrder>>): Trait {
    companion object: TraitType<OrderTrait>
    override val type: TraitType<*> = OrderTrait
}