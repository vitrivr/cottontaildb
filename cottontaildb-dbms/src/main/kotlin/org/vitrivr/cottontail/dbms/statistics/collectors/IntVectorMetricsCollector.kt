package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.IntVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class IntVectorMetricsCollector(val logicalSize: Int, config: MetricsConfig): RealVectorMetricsCollector<IntVectorValue>(Types.IntVector(logicalSize), config) {

    /** Local Metrics */
    val min: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MAX_VALUE })
    val max: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MIN_VALUE })
    val sum: IntVectorValue = IntVectorValue(IntArray(logicalSize))

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: IntVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                min.data[i] = Integer.min(d, min.data[i])
                max.data[i] = Integer.max(d, max.data[i])
                sum.data[i] += d
            }
        }
    }

    override fun calculate(probability: Float): IntVectorValueStatistics {
        val sampleMetrics = IntVectorValueStatistics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            min,
            max,
            sum
        )

        return IntVectorValueStatistics(1/probability,  sampleMetrics)

    }


}