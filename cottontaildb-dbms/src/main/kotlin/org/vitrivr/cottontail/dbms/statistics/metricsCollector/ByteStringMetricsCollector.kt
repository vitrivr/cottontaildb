package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteStringValueMetrics

class ByteStringMetricsCollector : AbstractScalarMetricsCollector<ByteStringValue>(Types.ByteString) {

    var minWidth : Int = Int.MAX_VALUE
    var maxWidth : Int = Int.MIN_VALUE
    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is ByteStringValue) {
            minWidth = Integer.min(value.logicalSize, minWidth)
            maxWidth = Integer.max(value.logicalSize, maxWidth)
        }
    }

    override fun calculate(): ByteStringValueMetrics {
        return ByteStringValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            minWidth,
            maxWidth
        )
    }


}