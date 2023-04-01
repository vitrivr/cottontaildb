package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.LongVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class LongVectorMetricsCollector(logicalSize: Int): RealVectorMetricsCollector<LongVectorValue, Long>(Types.LongVector(logicalSize)) {

    var min: LongArray = LongArray(logicalSize)
    var max: LongArray = LongArray(logicalSize)
    var sum: LongArray = LongArray(logicalSize)


    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is LongVectorValue) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max,
                min[i] = java.lang.Long.min(d, min[i])
                max[i] = java.lang.Long.min(d, max[i])
                sum[i] += d
                /*
                valueMetrics.min.data[i] = java.lang.Long.min(d, valueMetrics.min.data[i])
                valueMetrics.max.data[i] = java.lang.Long.max(d, valueMetrics.max.data[i])
                valueMetrics.sum.data[i] += d*/

            }
        }
    }

    override fun calculate(): LongVectorValueMetrics {
        TODO("Not yet implemented")
    }

}