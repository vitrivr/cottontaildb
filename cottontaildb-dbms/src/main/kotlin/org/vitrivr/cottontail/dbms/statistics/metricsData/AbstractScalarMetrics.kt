package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [ValueMetrics] implementation for all [Types.Scalar].
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractScalarMetrics<T: Value>(type: Types<T>): AbstractValueMetrics<T>(type) {

    companion object {
        const val MIN_WIDTH_KEY = "minWidth"
        const val MAX_WIDTH_KEY = "maxWidth"
    }

}