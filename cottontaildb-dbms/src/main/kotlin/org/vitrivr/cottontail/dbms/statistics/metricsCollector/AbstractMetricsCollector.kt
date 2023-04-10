package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import com.google.common.hash.BloomFilter
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics


/**
 * A basic implementation of a [MetricsCollector] object, which is used by Cottontail DB to collect
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
sealed class AbstractMetricsCollector<T : Value>(override val type: Types<T>) : MetricsCollector<T> {

    /** Init a BloomFilter*/
    abstract override val statisticsConfig: StatisticsConfig
    abstract override val expectedNumElements: Int
    private var bloomFilter : BloomFilter<Value> =  BloomFilter.create<Value>(Value.ValueFunnel, expectedNumElements, statisticsConfig.falsePositiveProbability)

    /** Global Metrics */
    override var numberOfDistinctEntries = 0L
    override var numberOfNonNullEntries = 0L
    override var numberOfNullEntries = 0L

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        if (value != null) {
            numberOfNonNullEntries += 1

            // BloomFilter check for distinct entries
            if (bloomFilter.mightContain(value)) {
                numberOfDistinctEntries += 1
            } else {
                bloomFilter.put(value)
            }

        } else {
            numberOfNullEntries += 1 // handle null case
        }
    }

    abstract override fun calculate(probability: Float) : AbstractValueMetrics<T>

}
