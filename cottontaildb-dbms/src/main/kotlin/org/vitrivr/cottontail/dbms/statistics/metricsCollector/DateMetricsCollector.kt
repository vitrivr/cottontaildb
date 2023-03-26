package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.DateValueMetrics
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [MetricsCollector] implementation for [DateValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DateMetricsCollector : AbstractScalarMetricsCollector<DateValue>(Types.Date) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: DateValueMetrics = DateValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is DateValue) {
            // set new min, max, and sum
            valueMetrics.min = DateValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = DateValue(max(value.value, valueMetrics.max.value))
        }
    }
}