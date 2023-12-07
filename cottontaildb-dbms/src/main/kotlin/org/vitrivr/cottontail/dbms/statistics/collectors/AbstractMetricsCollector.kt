package org.vitrivr.cottontail.dbms.statistics.collectors

import com.google.common.hash.BloomFilter
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.utilities.hashing.ValueFunnel
import kotlin.math.max

/**
 * A basic implementation of a [MetricsCollector] object, which is used by Cottontail DB to collect
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.3.0
 */
sealed class AbstractMetricsCollector<T : Value>(final override val type: Types<T>, final override val config: MetricsConfig) : MetricsCollector<T> {

    /** Init a BloomFilter*/
    private val expectedElements = max(this.config.expectedNumElements, 50000) // If we know nothing about the num of elements or if it's very small, we just assume 10000 entries

    /** [BloomFilter] instance used for estimating the number of distinct entries. */
    private val bloomFilter : BloomFilter<Value> =  BloomFilter.create<Value>(ValueFunnel, expectedElements, config.statisticsConfig.falsePositiveProbability)

    /** Number of distinct entries seen by this [MetricsCollector]. */
    override var numberOfDistinctEntries : Long = 0L

    /** Number of null entries seen by this [MetricsCollector]. */
    override var numberOfNonNullEntries : Long= 0L

    /** Number of non-null entries seen by this [MetricsCollector]. */
    override var numberOfNullEntries : Long= 0L

    /**
     * Receives a [Value] that should be considered for analysis.
     *
     * @param value The [Value] received.
     */
    override fun receive(value: T?) {
        if (value != null) {
            this.numberOfNonNullEntries += 1
            if (this.bloomFilter.put(value)) {
                this.numberOfDistinctEntries += 1
            }
        } else {
            this.numberOfNullEntries += 1
        }
    }

    /**
     * Generates and returns the [ValueStatistics] based on the current state of the [MetricsCollector].
     *
     * @return Generated [ValueStatistics]
     */
    abstract override fun calculate(probability: Float): ValueStatistics<T>

}
