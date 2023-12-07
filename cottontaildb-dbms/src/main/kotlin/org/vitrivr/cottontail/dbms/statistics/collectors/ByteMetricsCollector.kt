package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.values.ByteValueStatistics

/**
 * A [MetricsCollector] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ByteMetricsCollector (config: MetricsConfig) : RealMetricsCollector<ByteValue>(Types.Byte, config) {

    override fun calculate(probability: Float): ByteValueStatistics {
        val sampleMetrics =  ByteValueStatistics(
            this.numberOfNullEntries,
            this.numberOfNonNullEntries,
            this.numberOfDistinctEntries,
            ByteValue(this.min),
            ByteValue(this.max),
            DoubleValue(this.sum),
            DoubleValue(this.mean),
            DoubleValue(this.variance),
            DoubleValue(this.skewness),
            DoubleValue(this.kurtosis)
        )

        return ByteValueStatistics(1/probability, sampleMetrics)
    }
}