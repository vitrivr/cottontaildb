package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.LongVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class LongVectorMetricsCollector(logicalSize: Int): RealVectorMetricsCollector<LongVectorValue, Long>(Types.LongVector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: LongVectorValueMetrics = LongVectorValueMetrics(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null && value is LongVectorValue) {
            valueMetrics.numberOfNonNullEntries += 1
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                valueMetrics.min.data[i] = java.lang.Long.min(d, valueMetrics.min.data[i])
                valueMetrics.max.data[i] = java.lang.Long.max(d, valueMetrics.max.data[i])
                valueMetrics.sum.data[i] += d

                // add to distinctSet
                distinctSets[i].add(d) // store in corresponding numberOfDistinctEntries
            }
        } else {
            valueMetrics.numberOfNullEntries += 1
        }
    }

}