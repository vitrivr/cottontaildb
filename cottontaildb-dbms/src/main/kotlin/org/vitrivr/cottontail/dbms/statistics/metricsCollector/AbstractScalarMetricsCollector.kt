package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import com.google.common.hash.BloomFilter
import org.vitrivr.cottontail.core.values.types.ScalarValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractScalarMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics

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
sealed class AbstractScalarMetricsCollector<T: Value>(type: Types<T>) : AbstractMetricsCollector<T>(type) {

}

