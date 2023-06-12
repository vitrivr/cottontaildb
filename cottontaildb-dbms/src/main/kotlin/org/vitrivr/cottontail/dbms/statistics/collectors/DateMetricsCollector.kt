package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.dbms.statistics.values.DateValueStatistics
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [MetricsCollector] implementation for [DateValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DateMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<DateValue>(Types.Date, config) {

    /** Local Metrics */
    var min : Long = Long.MAX_VALUE
    var max : Long = Long.MIN_VALUE

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: DateValue?) {
        super.receive(value)
        if (value != null) {
            // set new min and max
            min = min(value.value, min)
            max = max(value.value, max)
        }
    }

    override fun calculate(probability: Float): DateValueStatistics {
        val sampleMetrics = DateValueStatistics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            DateValue(min),
            DateValue(max),
        )

        return DateValueStatistics(1/probability, sampleMetrics)
    }
}