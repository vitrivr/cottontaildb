package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import com.google.common.hash.BloomFilter
import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import kotlin.math.max


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
sealed class AbstractMetricsCollector<T : Value>(override val type: Types<T>, override val config: MetricsConfig) : MetricsCollector<T> {

    /** Init a BloomFilter*/
    private val expectedElements = max(config.expectedNumElements, 50000) // If we know nothing about the num of elements or if it's very small, we just assume 10000 entries
    private var bloomFilter : BloomFilter<Value> =  BloomFilter.create<Value>(Value.ValueFunnel, expectedElements, config.statisticsConfig.falsePositiveProbability)

    /** Global Metrics */
    override var numberOfDistinctEntries = 0L
    override var numberOfNonNullEntries = 0L
    override var numberOfNullEntries = 0L

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: T?) {
        if (value != null) {
            this.numberOfNonNullEntries += 1

            // BloomFilter check for distinct entries
            if (bloomFilter.put(value)) {
                this.numberOfDistinctEntries += 1
            }

        } else {
            this.numberOfNullEntries += 1 // handle null case
        }
    }

    abstract override fun calculate(probability: Float) : AbstractValueMetrics<T>

}
