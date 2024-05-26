package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.dbms.statistics.values.Complex64VectorValueStatistics

/**
 * A [MetricsCollector] implementation for [Complex64VectorValue]s.

 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Complex64VectorMetricsCollector(logicalSize: Int, config: MetricsConfig): AbstractVectorMetricsCollector<Complex64VectorValue>(Types.Complex64Vector(logicalSize), config) {
    /**
     * Generates and returns the [Complex64VectorValueStatistics] based on the current state of the [Complex64VectorMetricsCollector].
     *
     * @return Generated [Complex64VectorValueStatistics]
     */
    override fun calculate() = Complex64VectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong()
    )
}