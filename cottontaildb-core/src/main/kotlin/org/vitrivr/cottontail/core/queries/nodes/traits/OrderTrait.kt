package org.vitrivr.cottontail.core.queries.nodes.traits

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder

/**
 * A trait indicating, that the result produced by a query plan sorted.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class OrderTrait(val order: List<Pair<ColumnDef<*>, SortOrder>>): Trait {
    companion object: TraitType<OrderTrait>
    override val type: TraitType<*> = OrderTrait
}