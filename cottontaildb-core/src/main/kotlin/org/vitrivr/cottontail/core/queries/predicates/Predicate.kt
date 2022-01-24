package org.vitrivr.cottontail.core.queries.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.Node

/**
 * A general purpose [Predicate] us can be used in a Cottontail DB query. Generally, [Predicate]s
 * are assumed to operate on [Record]s and usually affect a set of [ColumnDef]s in that [Record].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Predicate : Node {
    /** An estimation of the CPU cost required to apply this [Predicate] to a single [Record]. */
    val atomicCpuCost: Float

    /** Set of [ColumnDef] that are affected by this [Predicate]. */
    val columns: Set<ColumnDef<*>>
}





