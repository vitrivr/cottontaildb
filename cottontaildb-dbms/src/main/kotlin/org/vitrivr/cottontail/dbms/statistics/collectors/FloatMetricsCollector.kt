package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.dbms.statistics.values.FloatValueStatistics

/**
 * A [MetricsCollector] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class FloatMetricsCollector(config: MetricsConfig) : RealMetricsCollector<FloatValue>(Types.Float, config) {

    override fun calculate(probability: Float): FloatValueStatistics {
        val sampleMetrics = FloatValueStatistics(
            this.numberOfNullEntries,
            this.numberOfNonNullEntries,
            this.numberOfDistinctEntries,
            FloatValue(this.min),
            FloatValue(this.max),
            DoubleValue(this.sum),
            DoubleValue(this.mean),
            DoubleValue(this.variance),
            DoubleValue(this.skewness),
            DoubleValue(this.kurtosis)
        )

        return FloatValueStatistics(1/probability, sampleMetrics)
    }

}