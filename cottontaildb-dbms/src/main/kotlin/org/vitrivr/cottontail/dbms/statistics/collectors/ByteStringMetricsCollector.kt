package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.dbms.statistics.values.ByteStringValueStatistics

/**
 * A [MetricsCollector] implementation for [ByteStringValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
class ByteStringMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<ByteStringValue>(Types.ByteString, config) {

    /** Minimum width of the [ByteStringValue]s. */
    var minWidth : Int = Int.MAX_VALUE
        private set

    /** Minimum width of the [ByteStringValue]s. */
    var maxWidth : Int = Int.MIN_VALUE
        private set

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: ByteStringValue?) {
        super.receive(value)
        if (value != null) {
            this.minWidth = Integer.min(value.logicalSize, this.minWidth)
            this.maxWidth = Integer.max(value.logicalSize, this.maxWidth)
        }
    }

    /**
     * Generates and returns the [ByteStringValueStatistics] based on the current state of the [ByteStringMetricsCollector].
     *
     * @return Generated [ByteStringValueStatistics]
     */
    override fun calculate() = ByteStringValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.minWidth,
        this.maxWidth
    )
}