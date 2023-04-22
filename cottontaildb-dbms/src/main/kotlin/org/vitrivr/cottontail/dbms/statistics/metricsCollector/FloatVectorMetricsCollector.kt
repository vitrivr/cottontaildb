package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.metricsData.FloatVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class FloatVectorMetricsCollector(val logicalSize: Int, config: MetricsConfig) : RealVectorMetricsCollector<FloatVectorValue>(Types.FloatVector(logicalSize), config) {

    /** Local Metrics */
    val min: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MAX_VALUE })
    val max: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MIN_VALUE })
    val sum: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize))

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: FloatVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                min.data[i] = java.lang.Float.min(d, min.data[i])
                max.data[i] = java.lang.Float.max(d, max.data[i])
                sum.data[i] += d
            }
        }
    }

    override fun calculate(probability: Float): FloatVectorValueMetrics {
        val sampleMetrics = FloatVectorValueMetrics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            min,
            max,
            sum
        )

        return FloatVectorValueMetrics(1/probability, sampleMetrics)
    }

}