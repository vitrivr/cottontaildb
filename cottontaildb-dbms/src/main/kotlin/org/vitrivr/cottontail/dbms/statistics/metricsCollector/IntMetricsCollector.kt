package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.IntValueMetrics

/**
 * A [MetricsCollector] implementation for [IntValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class IntMetricsCollector(config: MetricsConfig) : RealMetricsCollector<IntValue>(Types.Int, config) {

    override fun calculate(probability: Float): IntValueMetrics {
        val sampleMetrics = IntValueMetrics(
            this.numberOfNullEntries,
            this.numberOfNonNullEntries,
            this.numberOfDistinctEntries,
            IntValue(this.min),
            IntValue(this.max),
            DoubleValue(this.sum),
            DoubleValue(this.mean),
            DoubleValue(this.variance),
            DoubleValue(this.skewness),
            DoubleValue(this.kurtosis)
        )

        return IntValueMetrics(1/probability, sampleMetrics)
    }


}
