package org.vitrivr.cottontail.core.queries.predicates

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.nodes.Node
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost

/**
 * A [Predicate] is a [Node] that is being evaluated as part of a Cottontail DB query.
 *
 * Generally, [Predicate]s are assumed to operate on [Record]s and usually affect a set of [ColumnDef]s in that [Record].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
sealed interface Predicate : NodeWithCost {
    /** Set of [ColumnDef] that are accessed by this [Predicate]. */
    val columns: Set<ColumnDef<*>>
}





