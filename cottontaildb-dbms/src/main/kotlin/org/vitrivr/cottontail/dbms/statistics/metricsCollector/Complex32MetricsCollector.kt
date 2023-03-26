package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32ValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
class Complex32MetricsCollector(): AbstractScalarMetricsCollector<Complex32Value>(Types.Complex32) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: Complex32ValueMetrics = Complex32ValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    /*override fun receive(value: Value?) {
        super.receive(value)
        // don't need since default from abstract is enough
    }*/

}