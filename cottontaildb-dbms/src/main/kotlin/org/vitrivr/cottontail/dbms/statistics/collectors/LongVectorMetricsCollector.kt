package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.LongVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class LongVectorMetricsCollector(val logicalSize: Int, config: MetricsConfig): RealVectorMetricsCollector<LongVectorValue>(Types.LongVector(logicalSize), config) {

    /** Local Metrics */
    val min: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MAX_VALUE })
    val max: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MIN_VALUE })
    val sum: LongVectorValue = LongVectorValue(LongArray(logicalSize))

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: LongVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max,
                min.data[i] = java.lang.Long.min(d, min.data[i])
                max.data[i] = java.lang.Long.min(d, max.data[i])
                sum.data[i] += d
            }
        }
    }

    override fun calculate(probability: Float): LongVectorValueStatistics {

        val sampleMetrics = LongVectorValueStatistics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            min,
            max,
            sum
        )

        return LongVectorValueStatistics(1/probability,  sampleMetrics)

    }

}