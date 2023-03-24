package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.LongValueMetrics

/**
 * A [MetricsCollector] implementation for [LongValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class LongMetricsColelctor : RealMetricsCollector<LongValue>(Types.Long) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: LongValueMetrics = LongValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        TODO("Receive to storage not yet implemented")
    }

    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    override fun calculate() {
        TODO("Write to storage not yet implemented")
    }

}