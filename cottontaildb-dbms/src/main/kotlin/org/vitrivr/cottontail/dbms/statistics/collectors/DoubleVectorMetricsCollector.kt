package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.DoubleVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class DoubleVectorMetricsCollector(val logicalSize: Int, config: MetricsConfig) : RealVectorMetricsCollector<DoubleVectorValue>(Types.DoubleVector(logicalSize), config) {

    /** Local Metrics */
    val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MAX_VALUE })
    val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MIN_VALUE })
    val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: DoubleVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                min.data[i] = java.lang.Double.min(d, min.data[i])
                max.data[i] = java.lang.Double.max(d, max.data[i])
                sum.data[i] += d
            }
        }
    }

    override fun calculate(probability: Float): DoubleVectorValueStatistics {
        val sampleMetrics = DoubleVectorValueStatistics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            min,
            max,
            sum
        )

        return DoubleVectorValueStatistics(1/probability, sampleMetrics)
    }

}