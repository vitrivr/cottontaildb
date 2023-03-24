package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32ValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
class Complex32MetricsCollector(): AbstractMetricsCollector<Complex32Value>(Types.Complex32) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: Complex32ValueMetrics = Complex32ValueMetrics()

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