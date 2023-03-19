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

    /**
     * Collects the data necessary to estimate metrics for the corresponding  [DataMetrics].
     *
     * @return Unit
     */
    fun collector(): Unit

}