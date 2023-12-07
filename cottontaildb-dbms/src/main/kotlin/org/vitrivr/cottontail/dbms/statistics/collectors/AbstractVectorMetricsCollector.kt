package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A basic implementation of a [MetricsCollector] object, which is used by Cottontail DB to collect
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractVectorMetricsCollector<T: VectorValue<*>>(type: Types<T>, config: MetricsConfig) : AbstractMetricsCollector<T>(type, config) {

}

