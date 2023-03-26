package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.BooleanValueMetrics


/**
 * A [MetricsCollector] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanMetricsCollector: AbstractScalarMetricsCollector<BooleanValue>(Types.Boolean) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: BooleanValueMetrics = BooleanValueMetrics()

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is BooleanValue) {
            when (value.value) {
                // count true and false entries
                true -> valueMetrics.numberOfTrueEntries += 1
                false -> valueMetrics.numberOfFalseEntries += 1
            }
        }
    }

}