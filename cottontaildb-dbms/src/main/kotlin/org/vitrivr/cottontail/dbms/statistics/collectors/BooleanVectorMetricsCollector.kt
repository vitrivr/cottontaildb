package org.vitrivr.cottontail.dbms.statistics.collectors
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.dbms.statistics.values.BooleanVectorValueStatistics

/**
 * A [MetricsCollector] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanVectorMetricsCollector(val logicalSize: Int, config: MetricsConfig) : AbstractVectorMetricsCollector<BooleanVectorValue>(Types.BooleanVector(logicalSize), config) {

    private var numberOfTrueEntries: LongArray = LongArray(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: BooleanVectorValue?) {
        super.receive(value)
        if (value != null) {
            for ((i, d) in value.data.withIndex()) {
                if (d) {
                    numberOfTrueEntries[i] = numberOfTrueEntries[i] + 1
                } // numberOfFalseEntries is computed using numberOfNonNullEntries and numberOfTrueEntries
            }
        }
    }

    override fun calculate(probability: Float): BooleanVectorValueStatistics {

        val sampleMetrics = BooleanVectorValueStatistics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            numberOfTrueEntries
        )

        return BooleanVectorValueStatistics(1/probability, sampleMetrics)
    }


}