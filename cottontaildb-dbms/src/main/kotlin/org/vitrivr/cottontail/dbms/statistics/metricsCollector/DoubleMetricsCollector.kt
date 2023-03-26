package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.DoubleValueMetrics
import java.lang.Double.max
import java.lang.Double.min

/**
 * A [MetricsCollector] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DoubleMetricsCollector : RealMetricsCollector<DoubleValue>(Types.Double) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: DoubleValueMetrics = DoubleValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is DoubleValue) {
            // set new min, max, and sum
            valueMetrics.min = DoubleValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = DoubleValue(max(value.value, valueMetrics.max.value))
            valueMetrics.sum += DoubleValue(value.value)
        }
    }

}