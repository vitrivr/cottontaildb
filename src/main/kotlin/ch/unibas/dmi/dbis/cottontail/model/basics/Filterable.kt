package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate

/**
 * An objects that holds [Record] values and allows for filtering & scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Filterable {
    /**
     * Applies the provided action to each [Record]  that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The action that should be applied.
     */
    fun forEach(predicate: Predicate, action: (Record) -> Unit)

    /**
     * Applies the provided mapping function to each [Record] that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     */
    fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R>


    /**
     * Filters this [Filterable] thereby creating and returning a new [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Filterable]
     */
    fun filter(predicate: Predicate): Filterable
}


