package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32VectorValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.1
 */
class Complex32VectorMetricsCollector(logicalSize: Int): AbstractMetricsCollector<Complex32VectorValue>(Types.Complex32Vector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: Complex32VectorValueMetrics = Complex32VectorValueMetrics(logicalSize)

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