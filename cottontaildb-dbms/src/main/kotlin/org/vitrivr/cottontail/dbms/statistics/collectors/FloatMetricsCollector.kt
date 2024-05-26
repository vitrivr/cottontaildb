package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.dbms.statistics.values.FloatValueStatistics

/**
 * A [MetricsCollector] implementation for [FloatValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class FloatMetricsCollector(config: MetricsConfig) : AbstractRealMetricsCollector<FloatValue>(Types.Float, config) {

    /**
     * Generates and returns the [FloatValueStatistics] based on the current state of the [FloatMetricsCollector].
     *
     * @return Generated [FloatValueStatistics]
     */
    override fun calculate()=  FloatValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        FloatValue(this.min),
        FloatValue(this.max),
        DoubleValue(this.sum),
        DoubleValue(this.mean),
        DoubleValue(this.variance),
        DoubleValue(this.skewness),
        DoubleValue(this.kurtosis)
    )
}