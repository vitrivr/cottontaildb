package org.vitrivr.cottontail.core.queries.predicates

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Node

/**
 * A [Predicate] is a [Node] that is being evaluated as part of a Cottontail DB query.
 *
 * Generally, [Predicate]s are assumed to operate on [Record]s and usually affect a set of [ColumnDef]s in that [Record].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
sealed interface Predicate : Node {
    /** Set of [ColumnDef] that are accessed by this [Predicate]. */
    val columns: Set<ColumnDef<*>>

    /**
     * Creates a copy of this [Predicate].
     *
     * @return [Predicate]
     */
    override fun copy(): Predicate
}





