package org.vitrivr.cottontail.dbms.statistics.metricsCollector
import org.vitrivr.cottontail.config.StatisticsConfig
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
class BooleanVectorMetricsCollector(val logicalSize: Int, override val statisticsConfig : StatisticsConfig, override val expectedNumElements: Int) : AbstractVectorMetricsCollector<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

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

    override fun calculate(probability: Float): BooleanVectorValueMetrics {

        val sampleMetrics = BooleanVectorValueMetrics(
            logicalSize,
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            numberOfTrueEntries
        )

        return BooleanVectorValueMetrics(1/probability, sampleMetrics)
    }


}