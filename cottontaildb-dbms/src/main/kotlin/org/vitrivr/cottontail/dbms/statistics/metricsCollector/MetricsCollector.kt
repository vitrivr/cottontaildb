package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.ValueMetrics

/**
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */

sealed interface MetricsCollector <T : Value> {

    /**
     * Some variables to keep track of collection status etc.?
     * */


    /** The [Types] of [MetricsCollector]. */
    val type: Types<T>

    /**
     * The instance of the [StatisticsConfig] that is currently used
     */
    val statisticsConfig : StatisticsConfig

    /**
     * The expected number of elements that the Bloom Filter will see.
     * This should be the whole number of elements known from previous collections
     * The collector is responsible to reduce this number if it's in sampling mode (e.g., probability < 1 )
     */
    val expectedNumElements: Int

    /** Global Metrics */
    var numberOfDistinctEntries : Long
    var numberOfNonNullEntries : Long
    var numberOfNullEntries : Long

    /**
     * Receives the values for which to compute the statistics
     */
    fun receive(value: Value?): Unit


    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    fun calculate(probability: Float): ValueMetrics<T>


}