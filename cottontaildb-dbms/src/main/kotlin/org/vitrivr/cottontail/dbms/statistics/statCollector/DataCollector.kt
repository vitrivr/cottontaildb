package org.vitrivr.cottontail.dbms.statistics.statCollector

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.statData.DataMetrics

/**
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */

sealed interface DataCollector <T : Value> {

    /**
     * Some variables to keep track of collection status etc.?
     * */


    /** The [Types] of [DataCollector]. */
    val type: Types<T>

    val dataMetrics: DataMetrics<T>


    /**
     * Receives the values for which to compute the statistics
     */
    fun receive(value: Value?): Unit


    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    fun calculate(): Unit

}