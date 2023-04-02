package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [ValueMetrics] implementation for all Vector Values.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractVectorMetrics<T: Value>(type: Types<T>): AbstractValueMetrics<T>(type) {

}