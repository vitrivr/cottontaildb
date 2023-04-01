package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractVectorMetrics

/**
 * A basic implementation of a [MetricsCollector] object, which is used by Cottontail DB to collect
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractVectorMetricsCollector<T: Value, R>(type: Types<T>) : AbstractMetricsCollector<T>(type) {

    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    /*override fun calculate() {
        /*for ((i, hashset) in distinctSets.withIndex()) {
            valueMetrics.numberOfDistinctEntriesArray[i] = hashset.size.toLong() // write size of distinct entries into array
        }*/
        //TODO("Write to storage here!")
    }*/
}

