package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.dbms.statistics.values.Complex64VectorValueStatistics

/**
 * A [MetricsCollector] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorMetricsCollector(val logicalSize: Int, config: MetricsConfig): AbstractVectorMetricsCollector<Complex64VectorValue>(Types.Complex64Vector(logicalSize), config) {

    override fun calculate(probability: Float): Complex64VectorValueStatistics {

        val sampleMetrics = Complex64VectorValueStatistics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries
        )

        return Complex64VectorValueStatistics(1/probability, sampleMetrics)
    }


}