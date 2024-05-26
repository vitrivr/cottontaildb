package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.dbms.statistics.values.DateValueStatistics
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [MetricsCollector] implementation for [DateValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class DateMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<DateValue>(Types.Date, config) {

    /** Smallest [DateValue] encountered by this [DateMetricsCollector]. */
    var min : Long = Long.MAX_VALUE
        private set

    /** LArgest [DateValue] encountered by this [DateMetricsCollector]. */
    var max : Long = Long.MIN_VALUE
        private set

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: DateValue?) {
        super.receive(value)
        if (value != null) {
            this.min = min(value.value, this.min)
            this.max = max(value.value, this.max)
        }
    }

    /**
     * Generates and returns the [DateValueStatistics] based on the current state of the [DateMetricsCollector].
     *
     * @return Generated [DateValueStatistics]
     */
    override fun calculate() = DateValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        DateValue(this.min),
        DateValue(this.max),
    )
}