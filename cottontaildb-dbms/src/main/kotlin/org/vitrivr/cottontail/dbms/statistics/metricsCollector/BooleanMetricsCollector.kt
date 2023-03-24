package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.BooleanValueMetrics


/**
 * A [MetricsCollector] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanMetricsCollector: AbstractMetricsCollector<BooleanValue>(Types.Boolean) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: BooleanValueMetrics = BooleanValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        val boolean =  (value as BooleanValue)
        if (boolean != null) {
            // case: not null
            valueMetrics.numberOfNonNullEntries += 1

            // Update True and False Entries
            if (boolean.isEqual(BooleanValue.TRUE)) {
                valueMetrics.numberOfTrueEntries += 1
            } else if (boolean.isEqual(BooleanValue.FALSE)) {
                valueMetrics.numberOfFalseEntries += 1
            }

        } else {
            // case: null
            valueMetrics.numberOfNullEntries += 1
        }

        // for dataMetrics.numberOfDistinctEntries, we have to keep track of the entries.
        distinctSet.add(boolean)
    }

    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    override fun calculate() {
        valueMetrics.numberOfDistinctEntries = distinctSet.size.toLong()
        //TODO("Write to storage not yet implemented")
    }

}