package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.Complex32VectorValueMetrics

/**
 * A [MetricsCollector] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.1
 */
class Complex32VectorMetricsCollector(logicalSize: Int): AbstractVectorMetricsCollector<Complex32VectorValue, Float>(Types.Complex32Vector(logicalSize)) {

    /** The corresponding [valueMetrics] which stores all metrics for [Types] */
    val valueMetrics: Complex32VectorValueMetrics = Complex32VectorValueMetrics(logicalSize)

    override fun calculate(): Complex32VectorValueMetrics {
        TODO("Not yet implemented")
    }


}