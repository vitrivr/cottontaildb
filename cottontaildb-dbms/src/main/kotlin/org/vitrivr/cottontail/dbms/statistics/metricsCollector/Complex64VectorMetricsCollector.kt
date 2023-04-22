package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex64VectorValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorMetricsCollector(val logicalSize: Int, config: MetricsConfig): AbstractVectorMetricsCollector<Complex64VectorValue>(Types.Complex64Vector(logicalSize), config) {

    override fun calculate(probability: Float): Complex64VectorValueMetrics {

        val sampleMetrics = Complex64VectorValueMetrics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries
        )

        return Complex64VectorValueMetrics(1/probability, sampleMetrics)
    }


}