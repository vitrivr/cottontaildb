package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteStringValueMetrics

class ByteStringMetricsCollector : AbstractScalarMetricsCollector<ByteStringValue>(Types.ByteString) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: ByteStringValueMetrics = ByteStringValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is ByteStringValue) {
            valueMetrics.minWidth = Integer.min(value.logicalSize, valueMetrics.minWidth)
            valueMetrics.maxWidth = Integer.max(value.logicalSize, valueMetrics.maxWidth)
        }
    }


}