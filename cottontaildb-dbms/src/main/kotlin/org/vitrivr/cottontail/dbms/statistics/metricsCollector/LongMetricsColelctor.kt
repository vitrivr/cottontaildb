package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.LongValueMetrics
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [MetricsCollector] implementation for [LongValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class LongMetricsColelctor(override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : RealMetricsCollector<LongValue>(Types.Long) {

    /** Local Metrics */
    var min : Long = 0
    var max : Long = 0
    var sum : Double = 0.0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is LongValue) {
            // set new min, max, and sum
            min = min(value.value, min)
            max = max(value.value, max)
            sum += value.value
        }
    }

    override fun calculate(probability: Float): LongValueMetrics {
        val sampleMetrics = LongValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            LongValue(min),
            LongValue(max),
            DoubleValue(sum)
        )

        return LongValueMetrics(1/probability, sampleMetrics)
    }

}