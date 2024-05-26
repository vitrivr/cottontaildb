package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.dbms.statistics.values.Complex32VectorValueStatistics

/**
 * A [MetricsCollector] implementation for [Complex32VectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Complex32VectorMetricsCollector(logicalSize: Int, config: MetricsConfig): AbstractVectorMetricsCollector<Complex32VectorValue>(Types.Complex32Vector(logicalSize), config) {
    /**
     * Generates and returns the [Complex32VectorValueStatistics] based on the current state of the [Complex32VectorMetricsCollector].
     *
     * @return Generated [Complex32VectorValueStatistics]
     */
    override fun calculate() = Complex32VectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong()
    )
}