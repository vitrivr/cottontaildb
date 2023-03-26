package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.IntValueMetrics
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [MetricsCollector] implementation for [IntValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class IntMetricsCollector : RealMetricsCollector<IntValue>(Types.Int) {


    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: IntValueMetrics = IntValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is IntValue) {
            // set new min, max, and sum
            valueMetrics.min = IntValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = IntValue(max(value.value, valueMetrics.max.value))
            valueMetrics.sum += DoubleValue(value.value)
        }
    }


}
