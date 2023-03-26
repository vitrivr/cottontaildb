package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ShortValueMetrics
import com.google.common.primitives.Shorts.max
import com.google.common.primitives.Shorts.min

/**
 * A [MetricsCollector] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ShortMetricsCollector : RealMetricsCollector<ShortValue>(Types.Short) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: ShortValueMetrics = ShortValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is ShortValue) {
            // set new min, max, and sum
            valueMetrics.min = ShortValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = ShortValue(max(value.value, valueMetrics.max.value))
            valueMetrics.sum += DoubleValue(value.value)
        }
    }

}