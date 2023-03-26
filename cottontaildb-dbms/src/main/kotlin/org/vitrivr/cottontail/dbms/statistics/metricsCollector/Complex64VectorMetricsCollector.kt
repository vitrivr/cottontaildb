package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex64VectorValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorMetricsCollector(logicalSize: Int): AbstractVectorMetricsCollector<Complex64VectorValue, Double>(Types.Complex64Vector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    override val valueMetrics: Complex64VectorValueMetrics = Complex64VectorValueMetrics(logicalSize)

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null && value is Complex64VectorValue) {
            valueMetrics.numberOfNonNullEntries += 1
            for ((i, d) in value.data.withIndex()) {
                distinctSets[i].add(d) // store in corresponding numberOfDistinctEntries
            }
        } else {
            valueMetrics.numberOfNullEntries += 1
        }
    }


}