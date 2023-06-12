package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.statistics.values.StringValueStatistics
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [MetricsCollector] implementation for [StringValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.2.0
 */
class StringMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<StringValue>(Types.String, config) {

    /** Local Metrics */
    var minWidth : Int = Int.MAX_VALUE
    var maxWidth : Int = Int.MIN_VALUE

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: StringValue?) {
        super.receive(value)
        if (value != null) {
            minWidth = min(value.logicalSize, minWidth)
            maxWidth = max(value.logicalSize, maxWidth)
        }
    }

    override fun calculate(probability: Float): StringValueStatistics {
        val sampleMetrics = StringValueStatistics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            minWidth,
            maxWidth,
        )

        return  StringValueStatistics(1/probability, sampleMetrics)
    }

}