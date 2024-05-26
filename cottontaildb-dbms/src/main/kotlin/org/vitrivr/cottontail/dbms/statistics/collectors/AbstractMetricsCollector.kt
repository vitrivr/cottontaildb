package org.vitrivr.cottontail.dbms.statistics.collectors

import com.google.common.hash.BloomFilter
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.utilities.hashing.ValueFunnel

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
    /** [BloomFilter] instance used for estimating the number of distinct entries. */
    private val bloomFilter : BloomFilter<Value> =  BloomFilter.create<Value>(ValueFunnel, this.config.numberOfElements, this.config.statisticsConfig.falsePositiveProbability)

    /** Number of distinct entries seen by this [MetricsCollector]. */
    override var numberOfDistinctEntries : Long = 0L
        protected set

    /** Number of null entries seen by this [MetricsCollector]. */
    override var numberOfNonNullEntries : Long = 0L
        protected set

    /** Number of non-null entries seen by this [MetricsCollector]. */
    override var numberOfNullEntries : Long = 0L
        protected set

    /**
     * Receives a [Value] that should be considered for analysis.
     *
     * @param value The [Value] received.
     */
    override fun receive(value: T?) {
        if (value != null) {
            this.numberOfNonNullEntries += 1L
            if (this.bloomFilter.put(value)) {
                this.numberOfDistinctEntries += 1L
            }
        } else {
            this.numberOfNullEntries += 1L
        }
    }

    /**
     * Generates and returns the [ValueStatistics] based on the current state of the [MetricsCollector].
     *
     * @return Generated [ValueStatistics]
     */
    abstract override fun calculate(): ValueStatistics<T>
}
