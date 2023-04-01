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

    /** Local Metrics */
    private var numberOfTrueEntries = 0L
    private var numberOfFalseEntries = 0L

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is BooleanValue) {
            when (value.value) {
                // count true and false entries
                true -> numberOfTrueEntries += 1
                false -> numberOfFalseEntries += 1
            }
        }
    }

    override fun calculate(): BooleanValueMetrics {
        return BooleanValueMetrics(numberOfTrueEntries, numberOfFalseEntries, numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
        //TODO("Not yet implemented")
    }

}