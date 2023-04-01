package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32ValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
class Complex32MetricsCollector(): AbstractScalarMetricsCollector<Complex32Value>(Types.Complex32) {
    override fun calculate(): Complex32ValueMetrics {
        return Complex32ValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
        )
    }

}