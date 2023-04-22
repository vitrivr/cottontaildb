package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [MetricsCollector] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class ByteMetricsCollector (override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : RealMetricsCollector<ByteValue>(Types.Byte) {

    override fun calculate(probability: Float): ByteValueMetrics {
        val sampleMetrics =  ByteValueMetrics(
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

        return ByteValueMetrics(1/probability, sampleMetrics)
    }
}