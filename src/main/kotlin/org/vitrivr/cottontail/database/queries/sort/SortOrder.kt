package org.vitrivr.cottontail.database.queries.sort

import org.vitrivr.cottontail.model.basics.Record

/**
 * Enumeration that describes the sort order.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class SortOrder {
    ASCENDING,
    DESCENDING;

    /**
     * Wraps the given [Comparator] based on the [SortOrder].
     *
     * @param comparator The [Comparator] to wrap.
     * @return Wrapped [Comparator].
     */
    fun wrap(comparator: Comparator<Record>): Comparator<Record> = when (this) {
        ASCENDING -> comparator
        DESCENDING -> Comparator { r1, r2 -> -comparator.compare(r1, r2) }
    }
}