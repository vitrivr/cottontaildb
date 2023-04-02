package org.vitrivr.cottontail.dbms.statistics.metricsCollector
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.BooleanVectorValueMetrics

/**
 * A [MetricsCollector] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanVectorMetricsCollector(val logicalSize: Int) : AbstractVectorMetricsCollector<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

    private var numberOfTrueEntries: LongArray = LongArray(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is BooleanVectorValue) {
            for ((i, d) in value.data.withIndex()) {
                if (d) {
                    numberOfTrueEntries[i] = numberOfTrueEntries[i] + 1
                } // numberOfFalseEntries is computed using numberOfNonNullEntries and numberOfTrueEntries

            }
        }
    }

    override fun calculate(): BooleanVectorValueMetrics {
        return BooleanVectorValueMetrics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            numberOfTrueEntries
        )
    }


}