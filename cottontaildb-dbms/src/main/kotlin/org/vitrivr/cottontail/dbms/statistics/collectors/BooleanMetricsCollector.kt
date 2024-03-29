package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.dbms.statistics.values.BooleanValueStatistics

/**
 * A [MetricsCollector] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanMetricsCollector (config: MetricsConfig): AbstractScalarMetricsCollector<BooleanValue>(Types.Boolean, config) {

    /** Local Metrics */
    private var numberOfTrueEntries = 0L
    private var numberOfFalseEntries = 0L

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: BooleanValue?) {
        super.receive(value)
        if (value != null) {
            when (value.value) {
                // count true and false entries
                true -> this.numberOfTrueEntries += 1
                false -> this.numberOfFalseEntries += 1
            }
        }
    }

    /**
     * This function creates a [ValueMetrics] based on the collected metrics.
     * Before creating it, the values are calculated from the sample back to the whole population.
     */
    override fun calculate(probability: Float): BooleanValueStatistics {
        val sampleMetrics =  BooleanValueStatistics(
            numberOfTrueEntries,
            numberOfFalseEntries,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries // in the boolean setting, this can only be 2 (true or false).
        )
        return BooleanValueStatistics(1/probability, sampleMetrics)
    }

}