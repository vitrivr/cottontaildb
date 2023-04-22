package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteStringValueMetrics

/**
 * A [MetricsCollector] implementation for [ByteStringValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
class ByteStringMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<ByteStringValue>(Types.ByteString, config) {

    var minWidth : Int = Int.MAX_VALUE
    var maxWidth : Int = Int.MIN_VALUE
    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: ByteStringValue?) {
        super.receive(value)
        if (value != null) {
            minWidth = Integer.min(value.logicalSize, minWidth)
            maxWidth = Integer.max(value.logicalSize, maxWidth)
        }
    }

    override fun calculate(probability: Float): ByteStringValueMetrics {
        val sampleMetrics = ByteStringValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            minWidth,
            maxWidth
        )

        return ByteStringValueMetrics(1/probability, sampleMetrics)
    }


}