package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.dbms.statistics.values.ShortValueStatistics

/**
 * A [MetricsCollector] implementation for [ShortValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class ShortMetricsCollector (config: MetricsConfig) : AbstractRealMetricsCollector<ShortValue>(Types.Short, config) {
    /**
     * Generates and returns the [ShortValueStatistics] based on the current state of the [ShortMetricsCollector].
     *
     * @return Generated [ShortValueStatistics]
     */
    override fun calculate() = ShortValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        ShortValue(this.min),
        ShortValue(this.max),
        DoubleValue(this.sum),
        DoubleValue(this.mean),
        DoubleValue(this.variance),
        DoubleValue(this.skewness),
        DoubleValue(this.kurtosis)
    )
}