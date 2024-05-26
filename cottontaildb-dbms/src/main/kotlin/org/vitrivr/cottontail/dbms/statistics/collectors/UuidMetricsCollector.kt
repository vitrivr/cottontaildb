package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue
import org.vitrivr.cottontail.dbms.statistics.values.UuidValueStatistics

/**
 * A specialized [MetricsCollector] implementation for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @author Florian Burkhardt
 * @version 1.1.0
 */
class UuidMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<UuidValue>(Types.Uuid, config) {

    /**
     * Generates and returns the [UuidValueStatistics] based on the current state of the [UuidMetricsCollector].
     *
     * @return Generated [UuidValueStatistics]
     */
    override fun calculate()= UuidValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong()
    )
}