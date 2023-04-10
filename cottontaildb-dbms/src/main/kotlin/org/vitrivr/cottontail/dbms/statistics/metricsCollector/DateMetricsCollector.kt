package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.DateValueMetrics
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [MetricsCollector] implementation for [DateValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DateMetricsCollector(override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : AbstractScalarMetricsCollector<DateValue>(Types.Date) {

    /** Local Metrics */
    var min : Long = 0
    var max : Long = 0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is DateValue) {
            // set new min and max
            min = min(value.value, min)
            max = max(value.value, max)
        }
    }

    override fun calculate(probability: Float): DateValueMetrics {
        val sampleMetrics = DateValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            DateValue(min),
            DateValue(max),
        )

        return DateValueMetrics(1/probability, sampleMetrics)
    }
}