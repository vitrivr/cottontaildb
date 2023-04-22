package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32VectorValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.1
 */
class Complex32VectorMetricsCollector(val logicalSize: Int, config: MetricsConfig): AbstractVectorMetricsCollector<Complex32VectorValue>(Types.Complex32Vector(logicalSize), config) {

    override fun calculate(probability: Float): Complex32VectorValueMetrics {
        val sampleMetrics = Complex32VectorValueMetrics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries
        )

        return Complex32VectorValueMetrics(1/probability, sampleMetrics)
    }


}