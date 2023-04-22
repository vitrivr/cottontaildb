package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.ShortValueMetrics

/**
 * A [MetricsCollector] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ShortMetricsCollector (config: MetricsConfig) : RealMetricsCollector<ShortValue>(Types.Short, config) {

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