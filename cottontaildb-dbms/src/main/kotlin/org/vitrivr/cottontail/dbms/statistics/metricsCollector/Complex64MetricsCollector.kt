package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex64ValueMetrics


/**
 * A [MetricsCollector] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64MetricsCollector(): AbstractScalarMetricsCollector<Complex64Value>(Types.Complex64) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: Complex64ValueMetrics = Complex64ValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    /*override fun receive(value: Value?) {
        super.receive(value) // don't need it, since default from abstract is enough
    }*/

}