package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A [ValueStatistics] implementation for all [Types.Scalar].
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractScalarStatistics<T: Value>(type: Types<T>): AbstractValueStatistics<T>(type) {
    companion object {
        const val MIN_WIDTH_KEY = "min_width"
        const val MAX_WIDTH_KEY = "max_width"
    }
}