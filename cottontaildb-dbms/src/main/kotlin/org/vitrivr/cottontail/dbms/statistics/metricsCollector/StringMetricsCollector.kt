package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.StringValueMetrics
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [MetricsCollector] implementation for [StringValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.2.0
 */
class StringMetricsCollector : AbstractScalarMetricsCollector<StringValue>(Types.String) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: StringValueMetrics = StringValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is StringValue) {
            valueMetrics.minWidth = min(value.logicalSize, valueMetrics.minWidth)
            valueMetrics.maxWidth = max(value.logicalSize, valueMetrics.maxWidth)
        }
    }

}