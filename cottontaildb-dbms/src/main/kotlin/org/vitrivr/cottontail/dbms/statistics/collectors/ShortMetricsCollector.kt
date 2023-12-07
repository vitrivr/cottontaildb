package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.dbms.statistics.values.ShortValueStatistics

/**
 * A [MetricsCollector] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ShortMetricsCollector (config: MetricsConfig) : RealMetricsCollector<ShortValue>(Types.Short, config) {

    override fun calculate(probability: Float): ShortValueStatistics {
        val sampleMetrics = ShortValueStatistics(
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
        return ShortValueStatistics(1/probability, sampleMetrics)
    }

}