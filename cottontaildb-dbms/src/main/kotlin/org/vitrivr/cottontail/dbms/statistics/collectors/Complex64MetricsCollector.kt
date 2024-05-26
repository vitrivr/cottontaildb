package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.dbms.statistics.values.Complex64ValueStatistics


/**
 * A [MetricsCollector] implementation for [Complex64Value]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Complex64MetricsCollector(config: MetricsConfig): AbstractScalarMetricsCollector<Complex64Value>(Types.Complex64, config) {
    /**
     * Generates and returns the [Complex64ValueStatistics] based on the current state of the [Complex64MetricsCollector].
     *
     * @return Generated [Complex64ValueStatistics]
     */
    override fun calculate() = Complex64ValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong()
    )
}