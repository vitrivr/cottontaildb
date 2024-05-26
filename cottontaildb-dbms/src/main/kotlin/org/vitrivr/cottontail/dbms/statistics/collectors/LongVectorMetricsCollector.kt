package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.LongVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [LongVectorValue]s.
 *
 * @author Florian Burkhard
 * @author Ralph Gasser
 * @version 1.4.0
 */
class LongVectorMetricsCollector(logicalSize: Int, config: MetricsConfig): AbstractRealVectorMetricsCollector<LongVectorValue>(Types.LongVector(logicalSize), config) {
    /** The component-wise minimum. */
    val min: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MAX_VALUE })

    /** The component-wise maximum. */
    val max: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MIN_VALUE })

    /**
     * Receives a [LongVectorValue] that should be considered for analysis.
     *
     * @param value The [LongVectorValue] received.
     */
    override fun receive(value: LongVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                this.min.data[i] = kotlin.math.min(d, this.min.data[i])
                this.max.data[i] = kotlin.math.min(d, this.max.data[i])
                this.sum.data[i] += d.toDouble()
            }
        }
    }

    /**
     * Generates and returns the [LongVectorValueStatistics] based on the current state of the [LongVectorMetricsCollector].
     *
     * @return Generated [LongVectorValueStatistics]
     */
    override fun calculate() = LongVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.min,
        this.max,
        this.sum
    )
}