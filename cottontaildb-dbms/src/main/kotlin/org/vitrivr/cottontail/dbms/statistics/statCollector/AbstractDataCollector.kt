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
        const val ENTRIES_KEY = "entries"
        const val NULL_ENTRIES_KEY = "null_entries"
    }
    // TODO create corresponding ValueMetrics Object for this Collector to which the metrics are written etc.

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        TODO("Not yet implemented")
    }

    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    override fun calculate() {
        TODO("Not yet implemented")
    }

}
