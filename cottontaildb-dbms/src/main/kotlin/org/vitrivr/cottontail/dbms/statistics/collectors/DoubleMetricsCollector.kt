package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.values.DoubleValueStatistics

/**
 * A [MetricsCollector] implementation for [DoubleValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class DoubleMetricsCollector(config: MetricsConfig) : AbstractRealMetricsCollector<DoubleValue>(Types.Double, config) {

    /**
     * Generates and returns the [DoubleValueStatistics] based on the current state of the [DoubleMetricsCollector].
     *
     * @return Generated [DoubleValueStatistics]
     */
    override fun calculate() =  DoubleValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        DoubleValue(this.min),
        DoubleValue(this.max),
        DoubleValue(this.sum),
        DoubleValue(this.mean),
        DoubleValue(this.variance),
        DoubleValue(this.skewness),
        DoubleValue(this.kurtosis)
    )
}