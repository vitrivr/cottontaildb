package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.statistics.values.LongValueStatistics

/**
 * A [MetricsCollector] implementation for [LongValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class LongMetricsCollector(config: MetricsConfig) : AbstractRealMetricsCollector<LongValue>(Types.Long, config) {

    /**
     * Generates and returns the [LongValueStatistics] based on the current state of the [LongMetricsCollector].
     *
     * @return Generated [LongValueStatistics]
     */
    override fun calculate() = LongValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        LongValue(this.min),
        LongValue(this.max),
        DoubleValue(this.sum),
        DoubleValue(this.mean),
        DoubleValue(this.variance),
        DoubleValue(this.skewness),
        DoubleValue(this.kurtosis)
    )
}