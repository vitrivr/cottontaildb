package org.vitrivr.cottontail.client.iterators

import org.vitrivr.cottontail.core.database.ColumnDef

/**
 * An [Iterator] for [Tuple]s as returned by the [org.vitrivr.cottontail.client.SimpleClient]
 *
 * @author Ralph Gasser
 * @version 2.0.0
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
    val columns: List<ColumnDef<*>>

    /**
     * Drains this [TupleIterator] into a [List] of [Tuple]s.
     *
     * @return [List] of [Tuple]
     */
    fun drainToList(): List<Tuple>
}