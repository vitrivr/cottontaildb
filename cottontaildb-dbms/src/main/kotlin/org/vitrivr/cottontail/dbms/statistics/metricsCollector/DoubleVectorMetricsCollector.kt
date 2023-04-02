package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.DoubleVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DoubleVectorMetricsCollector(val logicalSize: Int) : RealVectorMetricsCollector<DoubleVectorValue>(Types.DoubleVector(logicalSize)) {

    /** Local Metrics */
    val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MAX_VALUE })
    val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MIN_VALUE })
    val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is DoubleVectorValue) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                min.data[i] = java.lang.Double.min(d, min.data[i])
                max.data[i] = java.lang.Double.max(d, max.data[i])
                sum.data[i] += d
            }
        }
    }

    override fun calculate(probability: Float): DoubleVectorValueMetrics {
        val sampleMetrics = DoubleVectorValueMetrics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            min,
            max,
            sum
        )

        return DoubleVectorValueMetrics(1/probability, sampleMetrics)
    }

}