package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.FloatVectorValueStatistics
import org.vitrivr.cottontail.dbms.statistics.values.LongVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [FloatVectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class FloatVectorMetricsCollector(logicalSize: Int, config: MetricsConfig) : AbstractRealVectorMetricsCollector<FloatVectorValue>(Types.FloatVector(logicalSize), config) {

    /** The component-wise minimum. */
    val min: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MAX_VALUE })

    /** The component-wise maximum. */
    val max: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MIN_VALUE })

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: FloatVectorValue?) {
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
    override fun calculate() = FloatVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.min,
        this.max,
        this.sum
    )
}