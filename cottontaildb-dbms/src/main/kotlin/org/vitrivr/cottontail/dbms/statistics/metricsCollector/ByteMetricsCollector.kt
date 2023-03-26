package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics
import com.google.common.primitives.SignedBytes.max
import com.google.common.primitives.SignedBytes.min
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [MetricsCollector] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ByteMetricsCollector : RealMetricsCollector<ByteValue>(Types.Byte) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: ByteValueMetrics = ByteValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is ByteValue) {
            // set new min, max, and sum
            valueMetrics.min = ByteValue(min(value.value, valueMetrics.min.value))
            valueMetrics.max = ByteValue(max(value.value, valueMetrics.max.value))
            valueMetrics.sum += DoubleValue(value.value)
        }
    }
}