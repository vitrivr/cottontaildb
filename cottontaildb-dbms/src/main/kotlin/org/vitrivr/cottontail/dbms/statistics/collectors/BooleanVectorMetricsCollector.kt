package org.vitrivr.cottontail.dbms.statistics.collectors
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.BooleanVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [BooleanVectorValue]s.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.4.0
 */
class BooleanVectorMetricsCollector(logicalSize: Int, config: MetricsConfig) : AbstractVectorMetricsCollector<BooleanVectorValue>(Types.BooleanVector(logicalSize), config) {

    /** The number of true entries per component. */
    private var numberOfTrueEntries: LongArray = LongArray(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: BooleanVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] += 1L
            }
        }
    }

    /**
     * Generates and returns the [BooleanVectorValueStatistics] based on the current state of the [BooleanVectorMetricsCollector].
     *
     * @return Generated [BooleanVectorValueStatistics]
     */
    override fun calculate() = BooleanVectorValueStatistics(
        this.type.logicalSize,
        (this.numberOfNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfNonNullEntries / this.config.sampleProbability).toLong(),
        (this.numberOfDistinctEntries / this.config.sampleProbability).toLong(),
        (this.numberOfTrueEntries.map { (it / this.config.sampleProbability).toLong() }.toLongArray())
    )
}