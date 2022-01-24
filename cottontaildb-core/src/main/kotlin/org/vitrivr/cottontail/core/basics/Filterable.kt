package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.queries.predicates.Predicate

/**
 * An objects that holds [Record] values and allows for filtering & filtered scanning operation
 * on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
interface Filterable {
    /**
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    fun canProcess(predicate: Predicate): Boolean

    /**
     * Filters this [Filterable] thereby creating and returning a new [Iterator] for all
     * the [Record]s contained in this [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Iterator]
     */
    fun filter(predicate: Predicate): Iterator<Record>
}


