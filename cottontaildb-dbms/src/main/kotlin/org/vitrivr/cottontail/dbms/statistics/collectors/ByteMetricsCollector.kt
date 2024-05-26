package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.values.ByteValueStatistics

/**
 * A [MetricsCollector] implementation for [ByteValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class ByteMetricsCollector(config: MetricsConfig) : AbstractRealMetricsCollector<ByteValue>(Types.Byte, config) {
    /**
     * Generates and returns the [ByteValueStatistics] based on the current state of the [ByteMetricsCollector].
     *
     * @return Generated [ByteValueStatistics]
     */
    override fun calculate() = ByteValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        ByteValue(this.min),
        ByteValue(this.max),
        DoubleValue(this.sum),
        DoubleValue(this.mean),
        DoubleValue(this.variance),
        DoubleValue(this.skewness),
        DoubleValue(this.kurtosis)
    )
}