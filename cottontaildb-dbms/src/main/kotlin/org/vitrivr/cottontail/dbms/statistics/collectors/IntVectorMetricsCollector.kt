package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.IntVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [IntVectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class IntVectorMetricsCollector(logicalSize: Int, config: MetricsConfig): AbstractRealVectorMetricsCollector<IntVectorValue>(Types.IntVector(logicalSize), config) {

    /** The component-wise minimum. */
    val min: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MAX_VALUE })

    /** The component-wise maximum. */
    val max: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MIN_VALUE })

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: IntVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                this.min.data[i] = kotlin.math.min(d, this.min.data[i])
                this.max.data[i] = kotlin.math.min(d, this.max.data[i])
                this.sum.data[i] += d.toDouble()
            }
        }
    }

    /**
     * Generates and returns the [IntVectorValueStatistics] based on the current state of the [IntVectorMetricsCollector].
     *
     * @return Generated [IntVectorValueStatistics]
     */
    override fun calculate() = IntVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.min,
        this.max,
        this.sum
    )
}