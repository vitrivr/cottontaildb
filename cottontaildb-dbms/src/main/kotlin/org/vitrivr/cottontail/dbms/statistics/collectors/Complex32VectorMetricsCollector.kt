package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.dbms.statistics.values.Complex32VectorValueStatistics

/**
 * A [MetricsCollector] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.1
 */
class Complex32VectorMetricsCollector(val logicalSize: Int, config: MetricsConfig): AbstractVectorMetricsCollector<Complex32VectorValue>(Types.Complex32Vector(logicalSize), config) {

    override fun calculate(probability: Float): Complex32VectorValueStatistics {
        val sampleMetrics = Complex32VectorValueStatistics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries
        )

        return Complex32VectorValueStatistics(1/probability, sampleMetrics)
    }


}