package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics
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

    /** Local Metrics */
    var min : Double = 0.0
    var max : Double = 0.0
    var sum : Double = 0.0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is DoubleValue) {
            // set new min, max, and sum
            min = min(value.value, min)
            max = max(value.value, max)
            sum += value.value
        }
    }

    override fun calculate(probability: Float): DoubleValueMetrics {
        val sampleMetrics = DoubleValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            DoubleValue(min),
            DoubleValue(max),
            DoubleValue(sum)
        )

        return DoubleValueMetrics(1/probability, sampleMetrics)
    }

}