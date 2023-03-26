package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.DoubleVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DoubleVectorMetricsCollector(logicalSize: Int) : RealVectorMetricsCollector<DoubleVectorValue, Double>(Types.DoubleVector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: DoubleVectorValueMetrics = DoubleVectorValueMetrics(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null && value is DoubleVectorValue) {
            valueMetrics.numberOfNonNullEntries += 1
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                valueMetrics.min.data[i] = java.lang.Double.min(d, valueMetrics.min.data[i])
                valueMetrics.max.data[i] = java.lang.Double.max(d, valueMetrics.max.data[i])
                valueMetrics.sum.data[i] += d

                // add to distinctSet
                distinctSets[i].add(d) // store in corresponding numberOfDistinctEntries
            }
        } else {
            valueMetrics.numberOfNullEntries += 1
        }
    }

}