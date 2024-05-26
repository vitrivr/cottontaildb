package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.dbms.statistics.values.IntValueStatistics

/**
 * A [MetricsCollector] implementation for [IntValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IntMetricsCollector(config: MetricsConfig) : AbstractRealMetricsCollector<IntValue>(Types.Int, config) {
    /**
     * Generates and returns the [IntValueStatistics] based on the current state of the [IntMetricsCollector].
     *
     * @return Generated [IntValueStatistics]
     */
    override fun calculate() = IntValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        IntValue(this.min),
        IntValue(this.max),
        DoubleValue(this.sum),
        DoubleValue(this.mean),
        DoubleValue(this.variance),
        DoubleValue(this.skewness),
        DoubleValue(this.kurtosis)
    )
}
