package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
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
class DoubleMetricsCollector(override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : RealMetricsCollector<DoubleValue>(Types.Double) {

    /** Local Metrics */
    var min : Double = 0.0
    var max : Double = 0.0
    var sum : Double = 0.0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: DoubleValue?) {
        super.receive(value)
        if (value != null) {
            // set new min, max, and sum
            this.min = min(value.value, this.min)
            this.max = max(value.value, this.max)
            this.sum += value.value
        }
    }

    override fun calculate(probability: Float): DoubleValueMetrics {
        val sampleMetrics = DoubleValueMetrics(
            this.numberOfNullEntries,
            this.numberOfNonNullEntries,
            this.numberOfDistinctEntries,
            DoubleValue(this.min),
            DoubleValue(this.max),
            DoubleValue(this.sum),
            DoubleValue(this.mean),
            DoubleValue(this.variance),
            DoubleValue(this.skewness),
            DoubleValue(this.kurtosis)
        )

        return DoubleValueMetrics(1/probability, sampleMetrics)
    }

}