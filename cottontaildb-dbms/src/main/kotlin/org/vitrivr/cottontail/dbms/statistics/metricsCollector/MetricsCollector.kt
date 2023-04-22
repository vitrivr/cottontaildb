package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
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

    /** The instance of [MetricsConfig] that is used to pass up variables to super classes */
    val config: MetricsConfig

    /** Global Metrics */
    var numberOfDistinctEntries : Long
    var numberOfNonNullEntries : Long
    var numberOfNullEntries : Long

    /**
     * Receives the values for which to compute the statistics
     */
    fun receive(value: T?): Unit


    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    fun calculate(probability: Float): ValueMetrics<T>


}