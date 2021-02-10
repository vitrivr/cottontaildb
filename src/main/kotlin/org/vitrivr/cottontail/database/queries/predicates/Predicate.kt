package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * A general purpose [Predicate] us can be used in a Cottontail DB query. Generally, [Predicate]s
 * are assumed to operate on [Record]s and usually affect a set of [ColumnDef]s in that [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
interface Predicate {
    /** An estimation of the CPU [Cost] required to apply this [Predicate] to a single [Record]. */
    val cost: Float

    /** Set of [ColumnDef] that are affected by this [Predicate]. */
    val columns: Set<ColumnDef<*>>

    /**
     * Executes late value binding using the given [QueryContext].
     *
     * @param context [QueryContext] to use to resolve this [Binding].
     */
    fun bind(context: QueryContext): Predicate

    /**
     * Calculates and returns the digest for this [Predicate]
     *
     * @return Digest of this [Predicate] as [Long]
     */
    fun digest(): Long
}





