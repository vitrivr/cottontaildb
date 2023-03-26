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
class BooleanVectorMetricsCollector(logicalSize: Int) : AbstractVectorMetricsCollector<BooleanVectorValue, Boolean>(Types.BooleanVector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: BooleanVectorValueMetrics = BooleanVectorValueMetrics(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null && value is BooleanVectorValue) {
            valueMetrics.numberOfNonNullEntries += 1
            for ((i, d) in value.data.withIndex()) {
                if (d) {
                    valueMetrics.numberOfTrueEntries[i] = valueMetrics.numberOfTrueEntries[i] + 1
                } // numberOfFalseEntries is computed using numberOfNonNullEntries and numberOfTrueEntries

                distinctSets[i].add(d) // store in corresponding numberOfDistinctEntries
            }
        } else {
            valueMetrics.numberOfNullEntries += 1
        }
    }




}