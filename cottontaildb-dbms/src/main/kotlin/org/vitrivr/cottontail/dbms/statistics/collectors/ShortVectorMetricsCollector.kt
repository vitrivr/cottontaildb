package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.ShortVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [ShortVectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class ShortVectorMetricsCollector(logicalSize: Int, config: MetricsConfig): AbstractRealVectorMetricsCollector<ShortVectorValue>(Types.ShortVector(logicalSize), config) {

    /** The component-wise minimum. */
    val min: ShortVectorValue = ShortVectorValue(ShortArray(logicalSize) { Short.MAX_VALUE })

    /** The component-wise maximum. */
    val max: ShortVectorValue = ShortVectorValue(ShortArray(logicalSize) { Short.MIN_VALUE })

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: ShortVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                if (this.min.data[i] > d) this.min.data[i] = d
                if (max.data[i] < d) this.max.data[i] = d
                this.sum.data[i] += d.toDouble()
            }
        }
    }

    /**
     * Generates and returns the [ShortVectorValueStatistics] based on the current state of the [ShortVectorMetricsCollector].
     *
     * @return Generated [ShortVectorValueStatistics]
     */
    override fun calculate() = ShortVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.min,
        this.max,
        this.sum
    )
}