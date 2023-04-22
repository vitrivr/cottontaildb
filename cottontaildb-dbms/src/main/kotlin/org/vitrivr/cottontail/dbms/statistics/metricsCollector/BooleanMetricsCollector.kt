package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
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
class BooleanMetricsCollector (override val statisticsConfig : StatisticsConfig,
                               override val expectedNumElements: Int): AbstractScalarMetricsCollector<BooleanValue>(Types.Boolean) {

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
                true -> numberOfTrueEntries += 1
                false -> numberOfFalseEntries += 1
            }
        }
    }

    /**
     * This function creates a [ValueMetrics] based on the collected metrics.
     * Before creating it, the values are calculated from the sample back to the whole population.
     */
    override fun calculate(probability: Float): BooleanValueMetrics {
        val sampleMetrics =  BooleanValueMetrics(
            numberOfTrueEntries,
            numberOfFalseEntries,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries // in the boolean setting, this can only be 2 (true or false).
        )
        return BooleanValueMetrics(1/probability, sampleMetrics)
    }

}