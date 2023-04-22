package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ShortValueMetrics
import com.google.common.primitives.Shorts.max
import com.google.common.primitives.Shorts.min
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics

/**
 * A [MetricsCollector] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ShortMetricsCollector (override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : RealMetricsCollector<ShortValue>(Types.Short) {

    /** Local Metrics */
    var min : Short = 0
    var max : Short = 0
    var sum : Double = 0.0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: ShortValue?) {
        super.receive(value)
        if (value != null) {
            // set new min, max, and sum
            this.min = min(value.value, this.min)
            this.max = max(value.value, this.max)
            this.sum += value.value
        }
    }

    override fun calculate(probability: Float): ShortValueMetrics {
        val sampleMetrics = ShortValueMetrics(
            this.numberOfNullEntries,
            this.numberOfNonNullEntries,
            this.numberOfDistinctEntries,
            ShortValue(this.min),
            ShortValue(this.max),
            DoubleValue(this.sum),
            DoubleValue(this.mean),
            DoubleValue(this.variance),
            DoubleValue(this.skewness),
            DoubleValue(this.kurtosis)
        )
        return ShortValueMetrics(1/probability, sampleMetrics)
    }

}