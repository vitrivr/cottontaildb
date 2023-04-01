package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import com.google.common.primitives.SignedBytes
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics
import com.google.common.primitives.SignedBytes.max
import com.google.common.primitives.SignedBytes.min
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics

/**
 * A [MetricsCollector] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ByteMetricsCollector : RealMetricsCollector<ByteValue>(Types.Byte) {

    /** Local Metrics */
    var min : Byte = 0
    var max : Byte = 0
    var sum : Double = 0.0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is ByteValue) {
            // set new min, max, and sum
            min = min(value.value, min)
            max = max(value.value, max)
            sum += value.value
        }
    }

    override fun calculate(): ByteValueMetrics {
        return  ByteValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            ByteValue(min),
            ByteValue(max),
            DoubleValue(sum)
        )
    }
}