package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.LongValueMetrics

/**
 * A [MetricsCollector] implementation for [LongValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class LongMetricsCollector(config: MetricsConfig) : RealMetricsCollector<LongValue>(Types.Long, config) {

    override fun calculate(probability: Float): LongValueMetrics {
        val sampleMetrics = LongValueMetrics(
            this.numberOfNullEntries,
            this.numberOfNonNullEntries,
            this.numberOfDistinctEntries,
            LongValue(this.min),
            LongValue(this.max),
            DoubleValue(this.sum),
            DoubleValue(this.mean),
            DoubleValue(this.variance),
            DoubleValue(this.skewness),
            DoubleValue(this.kurtosis)
        )

        return LongValueMetrics(1/probability, sampleMetrics)
    }

}