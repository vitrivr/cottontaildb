package org.vitrivr.cottontail.core.queries.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.nodes.Node
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.tuple.Tuple

/**
 * A [Predicate] is a [Node] that is being evaluated as part of a Cottontail DB query.
 *
 * Generally, [Predicate]s are assumed to operate on [Tuple]s and usually affect a set of [ColumnDef]s in that [Tuple].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
sealed interface Predicate : NodeWithCost {
    /** Set of [ColumnDef] that are accessed by this [Predicate]. */
    val columns: Set<ColumnDef<*>>
}





