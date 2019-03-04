package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * An objects that holds [Record] values and allows for filtering & filtered scanning operation on those [Record] values in a parallel manner.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ParallelFilterable {
    /**
     * Filters this [Filterable] thereby creating and returning a new [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Filterable]
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun filter(parallelism: Short = 2, predicate: Predicate): Filterable

    /**
     * Applies the provided action to each [Record]  that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The action that should be applied.
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun forEach(parallelism: Short = 2, predicate: Predicate, action: (Record) -> Unit)

    /**
     * Applies the provided mapping function to each [Record] that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun <R> map(parallelism: Short = 2, predicate: Predicate, action: (Record) -> R): Collection<R>
}