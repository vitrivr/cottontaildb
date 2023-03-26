package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.FloatValueMetrics
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [MetricsCollector] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class FloatMetricsCollector : RealMetricsCollector<FloatValue>(Types.Float) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: FloatValueMetrics = FloatValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is FloatValue) {
            // set new min, max, and sum
            valueMetrics.min = FloatValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = FloatValue(max(value.value, valueMetrics.max.value))
            valueMetrics.sum += DoubleValue(value.value)
        }
    }

}