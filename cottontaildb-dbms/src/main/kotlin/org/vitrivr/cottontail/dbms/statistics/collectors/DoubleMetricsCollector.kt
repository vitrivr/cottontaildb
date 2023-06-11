package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.values.DoubleValueStatistics

/**
 * A [MetricsCollector] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DoubleMetricsCollector(config: MetricsConfig) : RealMetricsCollector<DoubleValue>(Types.Double, config) {

    override fun calculate(probability: Float): DoubleValueStatistics {

        val sampleMetrics = DoubleValueStatistics(
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

        return DoubleValueStatistics(1/probability, sampleMetrics)
    }

}