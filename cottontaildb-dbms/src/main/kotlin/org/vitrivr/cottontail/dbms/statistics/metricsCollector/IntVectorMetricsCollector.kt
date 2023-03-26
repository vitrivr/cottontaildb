package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.IntVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IntVectorMetricsCollector(logicalSize: Int): RealVectorMetricsCollector<IntVectorValue, Int>(Types.IntVector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: IntVectorValueMetrics = IntVectorValueMetrics(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null && value is IntVectorValue) {
            valueMetrics.numberOfNonNullEntries += 1
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                valueMetrics.min.data[i] = Integer.min(d, valueMetrics.min.data[i])
                valueMetrics.max.data[i] = Integer.max(d, valueMetrics.max.data[i])
                valueMetrics.sum.data[i] += d

                // add to distinctSet
                distinctSets[i].add(d) // store in corresponding numberOfDistinctEntries
            }
        } else {
            valueMetrics.numberOfNullEntries += 1
        }
    }



}