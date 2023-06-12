package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.dbms.statistics.values.Complex32ValueStatistics

/**
 * A [MetricsCollector] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
class Complex32MetricsCollector(config: MetricsConfig): AbstractScalarMetricsCollector<Complex32Value>(Types.Complex32, config) {
    override fun calculate(probability: Float): Complex32ValueStatistics {
        val sampleMetrics = Complex32ValueStatistics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
        )

        return Complex32ValueStatistics(1/probability, sampleMetrics)
    }

}