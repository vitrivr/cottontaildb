package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex64ValueMetrics


/**
 * A [MetricsCollector] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64MetricsCollector(config: MetricsConfig): AbstractScalarMetricsCollector<Complex64Value>(Types.Complex64, config) {

    override fun calculate(probability: Float): Complex64ValueMetrics {
        val sampleMetrics = Complex64ValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
        )

        return Complex64ValueMetrics(1/probability, sampleMetrics)
    }

}