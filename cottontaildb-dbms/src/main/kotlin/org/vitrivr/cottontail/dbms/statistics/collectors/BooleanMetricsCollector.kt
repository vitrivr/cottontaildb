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

    /** Number of entries that have the value [BooleanValue.TRUE]. */
    private var numberOfTrueEntries = 0L

    /** Number of entries that have the value [BooleanValue.FALSE]. */
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
     * Creates a [BooleanValueStatistics] based on this [BooleanMetricsCollector].
     *
     * @return [BooleanValueStatistics]
     */
    override fun calculate(): BooleanValueStatistics = BooleanValueStatistics(
        (numberOfTrueEntries / this.config.sampleProbability).toLong(),
        (numberOfFalseEntries / this.config.sampleProbability).toLong(),
        (numberOfNullEntries / this.config.sampleProbability).toLong(),
        (numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (numberOfDistinctEntries / this.config.sampleProbability).toLong() // in the boolean setting, this can only be 2 (true or false).
    )
}