package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32VectorValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.1
 */
class Complex32VectorMetricsCollector(logicalSize: Int): AbstractVectorMetricsCollector<Complex32VectorValue, Float>(Types.Complex32Vector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: Complex32VectorValueMetrics = Complex32VectorValueMetrics(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null && value is Complex32VectorValue) {
            valueMetrics.numberOfNonNullEntries += 1
            for ((i, d) in value.data.withIndex()) {
                distinctSets[i].add(d) // store in corresponding numberOfDistinctEntries
            }
        } else {
            valueMetrics.numberOfNullEntries += 1
        }
    }


}