package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.DoubleVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [DoubleVectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class DoubleVectorMetricsCollector(val logicalSize: Int, config: MetricsConfig) : AbstractRealVectorMetricsCollector<DoubleVectorValue>(Types.DoubleVector(logicalSize), config) {

    /** The component-wise minimum. */
    val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MAX_VALUE })

    /** The component-wise maximum. */
    val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MIN_VALUE })

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: DoubleVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                // update min, max, sum
                min.data[i] = kotlin.math.min(d, min.data[i])
                max.data[i] = kotlin.math.min(d, max.data[i])
                sum.data[i] += d
            }
        }
    }

    /**
     * Generates and returns the [DoubleVectorValueStatistics] based on the current state of the [DoubleVectorMetricsCollector].
     *
     * @return Generated [DoubleVectorValueStatistics]
     */
    override fun calculate() = DoubleVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.min,
        this.max,
        this.sum
    )
}