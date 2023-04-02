package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics
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

    /** Local Metrics */
    var min : Float = 0.0F
    var max : Float = 0.0F
    var sum : Double = 0.0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is FloatValue) {
            // set new min, max, and sum
            min = min(value.value, min)
            max = max(value.value, max)
            sum += value.value
        }
    }

    override fun calculate(probability: Float): FloatValueMetrics {
        val sampleMetrics = FloatValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            FloatValue(min),
            FloatValue(max),
            DoubleValue(sum)
        )

        return FloatValueMetrics(1/probability, sampleMetrics)
    }

}