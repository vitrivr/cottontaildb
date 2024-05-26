package org.vitrivr.cottontail.dbms.statistics.collectors


import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue

/**
 * A [MetricsCollector] for [RealVectorValue]s
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
sealed class AbstractRealVectorMetricsCollector<T: RealVectorValue<*>>(type: Types<T>, config: MetricsConfig): AbstractVectorMetricsCollector<T>(type, config) {
    /** The component-wise sum. */
    val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(type.logicalSize))
}