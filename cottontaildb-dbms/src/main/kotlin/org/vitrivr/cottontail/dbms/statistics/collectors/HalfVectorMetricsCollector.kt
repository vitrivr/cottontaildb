package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.HalfVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.HalfVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [HalfVectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class HalfVectorMetricsCollector(logicalSize: Int, config: MetricsConfig) : AbstractRealVectorMetricsCollector<HalfVectorValue>(Types.HalfVector(logicalSize), config) {

    /** The component-wise minimum. */
    val min: HalfVectorValue = HalfVectorValue(FloatArray(logicalSize) { Float.MAX_VALUE })

    /** The component-wise maximum. */
    val max: HalfVectorValue = HalfVectorValue(FloatArray(logicalSize) { Float.MIN_VALUE })

    /**
     * Generates and returns the [HalfVectorValueStatistics] based on the current state of the [HalfVectorMetricsCollector].
     *
     * @return Generated [HalfVectorValueStatistics]
     */
    override fun calculate() = HalfVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.min,
        this.max,
        this.sum
    )
}