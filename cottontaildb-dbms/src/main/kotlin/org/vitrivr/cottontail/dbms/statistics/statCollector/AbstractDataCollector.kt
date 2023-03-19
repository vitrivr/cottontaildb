package org.vitrivr.cottontail.dbms.statistics.statCollector

import org.vitrivr.cottontail.dbms.statistics.statData.DataMetrics
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value


/**
 * A basic implementation of a [DataCollector] object, which is used by Cottontail DB to collect
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
sealed class AbstractDataCollector<T : Value>(override val type: Types<T>) : DataCollector<T> {
    companion object {
        const val FRESH_KEY = "fresh"
        const val ENTRIES_KEY = "entries"
        const val NULL_ENTRIES_KEY = "null_entries"
    }

    /**
     * Collects the data necessary to estimate metrics for the corresponding  [DataMetrics].
     *
     * @return Unit
     */
    override fun collector() {
        TODO("Not yet implemented")
    }

}
