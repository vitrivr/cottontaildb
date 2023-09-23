package org.vitrivr.cottontail.dbms.statistics.collectors


import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.RealVectorValue

/**
 * A [MetricsCollector] for [VectorValue]s
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
sealed class RealVectorMetricsCollector<T: RealVectorValue<*>>(type: Types<T>, config: MetricsConfig): AbstractVectorMetricsCollector<T>(type, config) {


}