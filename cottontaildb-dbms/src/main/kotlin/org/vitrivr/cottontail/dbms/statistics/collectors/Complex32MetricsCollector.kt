package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.dbms.statistics.values.Complex32ValueStatistics

/**
 * A [MetricsCollector] implementation for [Complex32Value]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Complex32MetricsCollector(config: MetricsConfig): AbstractScalarMetricsCollector<Complex32Value>(Types.Complex32, config) {
    /**
     * Generates and returns the [Complex32MetricsCollector] based on the current state of the [Complex32MetricsCollector].
     *
     * @return Generated [Complex32MetricsCollector]
     */
    override fun calculate() = Complex32ValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong()
    )
}