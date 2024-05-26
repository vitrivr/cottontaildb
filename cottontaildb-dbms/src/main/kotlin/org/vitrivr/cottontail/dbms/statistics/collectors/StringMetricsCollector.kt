package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.statistics.values.StringValueStatistics
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [MetricsCollector] implementation for [StringValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.3.0
 */
class StringMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<StringValue>(Types.String, config) {

    /** Minimum width of the [StringValue]s. */
    var minWidth : Int = Int.MAX_VALUE
        private set

    /** Maximum width of the [StringValue]s. */
    var maxWidth : Int = Int.MIN_VALUE
        private set

    /**
     * Receives a [StringValue] that should be considered for analysis.
     *
     * @param value The [StringValue] received.
     */
    override fun receive(value: StringValue?) {
        super.receive(value)
        if (value != null) {
            this.minWidth = min(value.logicalSize, this.minWidth)
            this.maxWidth = max(value.logicalSize, this.maxWidth)
        }
    }

    /**
     * Generates and returns the [StringValueStatistics] based on the current state of the [StringMetricsCollector].
     *
     * @return Generated [StringValueStatistics]
     */
    override fun calculate() = StringValueStatistics(
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        this.minWidth,
        this.maxWidth
    )
}