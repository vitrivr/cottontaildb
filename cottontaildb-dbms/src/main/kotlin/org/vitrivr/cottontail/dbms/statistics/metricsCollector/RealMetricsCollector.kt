package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types

/**
 * A [MetricsCollector] implementation for [RealValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
sealed class RealMetricsCollector<T: RealValue<*>>(type: Types<T>): AbstractMetricsCollector<T>(type) {

}