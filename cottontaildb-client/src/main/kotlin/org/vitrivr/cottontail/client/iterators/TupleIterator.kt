package org.vitrivr.cottontail.client.iterators

import org.vitrivr.cottontail.core.types.Types

/**
 * An [Iterator] for [Tuple]s as returned by the [org.vitrivr.cottontail.client.SimpleClient]
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
interface TupleIterator : Iterator<Tuple>, AutoCloseable {
    /** The ID of the Cottontail DB transaction this [TupleIterator] is associated with. */
    val transactionId: Long

    /** The ID of the Cottontail DB query this [TupleIterator] is associated with. */
    val queryId: String

    /** The ID of the Cottontail DB query this [TupleIterator] is associated with. */
    val planDuration: Long

    /** The ID of the Cottontail DB query this [TupleIterator] is associated with. */
    val queryDuration: Long

    /** Number of columns returned by this [TupleIterator]. */
    val numberOfColumns: Int

    /** [List] of column names returned by this [TupleIterator] in order of occurrence. Contains fully qualified names. */
    val columnNames: List<String>

    /**
     * [List] of simple column names returned by this [TupleIterator] in order of occurrence.
     *
     * Since simple names may collide, list may be incomplete for given query.
     */
    val simpleNames: List<String>

    /** [List] of column [Types] returned by this [TupleIterator] in order of occurrence. Contains fully qualified names. */
    val columnTypes: List<Types<*>>
}