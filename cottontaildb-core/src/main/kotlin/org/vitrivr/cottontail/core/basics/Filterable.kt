package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.tuple.Tuple

/**
 * An objects that holds [Tuple] values and allows for filtering & filtered scanning operation
 * on those [Tuple] values.
 *
 * @see Tuple
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Filterable {
    /** True, if the [Filterable] supports filtering a partition. */
    val supportsPartitioning: Boolean

    /**
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    fun canProcess(predicate: Predicate): Boolean

    /**
     * Filters this [Filterable] thereby creating and returning a new [Cursor] for all
     * the [Tuple]s contained in this [Filterable].
     *
     * @param predicate [Predicate] to filter with.
     * @return New [Cursor]
     */
    fun filter(predicate: Predicate): Cursor<Tuple>

    /**
     * Filters this [Filterable] thereby creating and returning a new [Cursor] for all the [Tuple]s
     * contained in this [Filterable] that fall into the specified partition.
     *
     * @param predicate [Predicate] to filter with.
     * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
     * @return New [Cursor]
     */
    fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple>

}


