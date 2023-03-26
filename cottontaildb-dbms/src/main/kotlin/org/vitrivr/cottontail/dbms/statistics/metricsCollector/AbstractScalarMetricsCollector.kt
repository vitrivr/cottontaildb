package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractScalarMetrics
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
sealed class AbstractScalarMetricsCollector<T: Value>(type: Types<T>) : AbstractMetricsCollector<T>(type) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    abstract override val valueMetrics: AbstractScalarMetrics<T>

    /** HashMap to count the distinct values */
    var distinctSet = HashSet<Value>()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null) {
            valueMetrics.numberOfNonNullEntries += 1
            distinctSet.add(value) // store for numberOfDistinctEntries
        } else {
            valueMetrics.numberOfNullEntries += 1 // handle null case
        }
   }

    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    override fun calculate() {
        valueMetrics.numberOfDistinctEntries = distinctSet.size.toLong() + if (valueMetrics.numberOfNullEntries > 0) 1 else 0 // since we don't keep track in distinctSet of null, if there are null entries we have to add 1 here
        //TODO("Write to storage here!")
    }

}

