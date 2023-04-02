package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics

/**
 * A [MetricsCollector] for [VectorValue]s
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
sealed class RealVectorMetricsCollector<T: RealVectorValue<*>>(type: Types<T>): AbstractVectorMetricsCollector<T>(type) {

}