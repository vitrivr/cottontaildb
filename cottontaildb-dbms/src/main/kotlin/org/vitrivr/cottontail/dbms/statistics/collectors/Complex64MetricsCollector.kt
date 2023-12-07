package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.dbms.statistics.values.Complex64ValueStatistics


/**
 * A [MetricsCollector] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64MetricsCollector(config: MetricsConfig): AbstractScalarMetricsCollector<Complex64Value>(Types.Complex64, config) {

    override fun calculate(probability: Float): Complex64ValueStatistics {
        val sampleMetrics = Complex64ValueStatistics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
        )

        return Complex64ValueStatistics(1/probability, sampleMetrics)
    }

}