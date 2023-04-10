package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.StringValueMetrics
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [MetricsCollector] implementation for [StringValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.2.0
 */
class StringMetricsCollector(override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : AbstractScalarMetricsCollector<StringValue>(Types.String) {

    /** Local Metrics */
    var minWidth : Int = 0
    var maxWidth : Int = 0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is StringValue) {
            minWidth = min(value.logicalSize, minWidth)
            maxWidth = max(value.logicalSize, maxWidth)
        }
    }

    override fun calculate(probability: Float): StringValueMetrics {
        val sampleMetrics = StringValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            minWidth,
            maxWidth,
        )

        return  StringValueMetrics(1/probability, sampleMetrics)
    }

}