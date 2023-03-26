package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.LongValueMetrics
import java.lang.Long.max
import java.lang.Long.min

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
        super.receive(value)
        if (value != null && value is LongValue) {
            // set new min, max, and sum
            valueMetrics.min = LongValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = LongValue(max(value.value, valueMetrics.max.value))
            valueMetrics.sum += DoubleValue(value.value)
        }
    }

}